/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.streamnode.engine.scheduler.task;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.engine.computation.IStreamComputeTask;
import org.apache.iotdb.streamnode.engine.sink.IStreamSinkTask;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask;
import org.apache.iotdb.streamnode.engine.window.IEventInfo;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.apache.tsfile.read.common.block.TsBlock;

import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.apache.iotdb.calc.execution.operator.Operator.NOT_BLOCKED;

public class StreamDriver implements IStreamDriver {
  private final StreamDriverContext driverContext;

  private final StreamSubTask streamSubTask;

  private IEventInfo currentEvent;
  private ListenableFuture<?> pendingSinkFuture = NOT_BLOCKED;
  private final AtomicReference<SettableFuture<?>> driverBlockedFuture = new AtomicReference<>();
  private final AtomicReference<State> state = new AtomicReference<>(State.ALIVE);
  private final DriverLock exclusiveLock = new DriverLock();

  private enum State {
    ALIVE,
    NEED_DESTRUCTION,
    DESTROYED
  }

  public StreamDriver(StreamSubTask streamSubTask, StreamDriverContext driverContext) {
    this.streamSubTask = checkNotNull(streamSubTask, "stream sub task should not be null");
    this.driverContext = checkNotNull(driverContext, "driver context should not be null");

    // initially the driverBlockedFuture is not blocked (it is completed)
    SettableFuture<Void> future = SettableFuture.create();
    future.set(null);
    driverBlockedFuture.set(future);
  }

  @Override
  public boolean isFinished() {
    checkLockNotHeld("Cannot check finished status while holding the driver lock");
    Optional<Boolean> result = tryWithLockUninterruptibly(this::isFinishedInternal);
    return result.orElseGet(() -> state.get() != State.ALIVE || driverContext.isDone());
  }

  @Override
  public ListenableFuture<?> processFor(Duration duration) {
    SettableFuture<?> blockedFuture = driverBlockedFuture.get();

    // if the driver is blocked we don't need to continue
    if (!blockedFuture.isDone()) {
      return blockedFuture;
    }
    final long maxRuntime = duration.roundTo(TimeUnit.NANOSECONDS);
    Optional<ListenableFuture<?>> result =
        tryWithLock(
            100,
            TimeUnit.MILLISECONDS,
            false,
            () -> {
              if (state.get() != State.ALIVE) {
                return NOT_BLOCKED;
              }
              final long startNanos = System.nanoTime();
              do {
                ListenableFuture<?> future = processInternal();
                if (!future.isDone()) {
                  return updateDriverBlockedFuture(future);
                }
              } while (state.get() == State.ALIVE
                  && System.nanoTime() - startNanos < maxRuntime
                  && !isFinishedInternal());

              return NOT_BLOCKED;
            });

    return result.orElse(NOT_BLOCKED);
  }

  private ListenableFuture<?> updateDriverBlockedFuture(ListenableFuture<?> sourceBlockedFuture) {
    // driverBlockedFuture will be completed as soon as the sourceBlockedFuture is completed
    // or any of the operators gets a memory revocation request
    SettableFuture<?> newDriverBlockedFuture = SettableFuture.create();
    driverBlockedFuture.set(newDriverBlockedFuture);
    sourceBlockedFuture.addListener(
        () -> {
          if (!newDriverBlockedFuture.isDone()) {
            newDriverBlockedFuture.set(null);
          }
        },
        directExecutor());

    // Although we don't have memory management for operator now, we should consider it for
    // future
    // it's possible that memory revoking is requested for some operator
    // before we update driverBlockedFuture above and we don't want to miss that
    // notification, so we check to see whether that's the case before returning.

    return newDriverBlockedFuture;
  }

  @SuppressWarnings({"squid:S1181", "squid:S112"})
  private ListenableFuture<?> processInternal() {
    try {
      IStreamComputeTask computeTask = streamSubTask.getComputeTask();
      IStreamSinkTask sinkTask = streamSubTask.getSink();

      if (pendingSinkFuture != null && !pendingSinkFuture.isDone()) {
        return pendingSinkFuture;
      }

      if (currentEvent == null) {
        ListenableFuture<?> windowBlockedFuture = getWindowBlockedFuture();
        if (!windowBlockedFuture.isDone()) {
          return windowBlockedFuture;
        }
        currentEvent = getEvent();
        driverContext.setCurrentEventInfo(currentEvent);
        computeTask.bindEventInfo(currentEvent);
      }

      ListenableFuture<?> computeTaskBlocked = computeTask.isBlocked();
      if (!computeTaskBlocked.isDone()) {
        return computeTaskBlocked;
      }

      TsBlock result = computeTask.compute();
      boolean finishedForCurrentEvent = computeTask.isFinished();
      if (finishedForCurrentEvent) {
        // The final output of an event can be null or empty. Sink should still accept this
        // commit-only push and commit the source data consumed by this event.
        pendingSinkFuture = sinkTask.push(result, currentEvent.getCommitIds());
        computeTask.reset();
        currentEvent = null;
        driverContext.setCurrentEventInfo(null);
      } else {
        if (result == null || result.isEmpty()) {
          return NOT_BLOCKED;
        }
        pendingSinkFuture = sinkTask.push(result, Collections.emptyList());
      }
      if (!pendingSinkFuture.isDone()) {
        return pendingSinkFuture;
      }
      return NOT_BLOCKED;
    } catch (Throwable t) {
      Throwable actualCause = t;
      if (actualCause.getCause() instanceof IoTDBRuntimeException) {
        actualCause = actualCause.getCause();
      }

      List<StackTraceElement> interrupterStack = exclusiveLock.getInterrupterStack();
      if (interrupterStack == null) {
        driverContext.failed(actualCause);
        if (actualCause instanceof RuntimeException) {
          throw (RuntimeException) actualCause;
        }
        throw new RuntimeException(actualCause);
      }

      Exception exception = new Exception("Interrupted By");
      exception.setStackTrace(interrupterStack.toArray(new StackTraceElement[0]));
      RuntimeException newException =
          new RuntimeException("StreamDriver was interrupted", exception);
      newException.addSuppressed(actualCause);
      driverContext.failed(newException);
      throw newException;
    }
  }

  @Override
  public ListenableFuture<?> push(TsBlock tsBlock, long commitId) {
    // TODO: Push the incoming TsBlock into WindowEngine/source buffer and record the commitId so
    // it can be committed after the corresponding window event is fully processed.
    return NOT_BLOCKED;
  }

  @Override
  public void close() {
    if (!state.compareAndSet(State.ALIVE, State.NEED_DESTRUCTION)) {
      return;
    }
    exclusiveLock.interruptCurrentOwner();
    tryWithLockUninterruptibly(() -> true);
  }

  @Override
  public void failed(Throwable t) {
    driverContext.failed(t);
  }

  @Override
  public DriverTaskId getDriverTaskId() {
    return driverContext.getDriverTaskId();
  }

  @Override
  public void setDriverTaskId(DriverTaskId driverTaskId) {
    driverContext.setDriverTaskId(driverTaskId);
  }

  private IEventInfo getEvent() {
    streamSubTask.getWindowEngine();
    return null;
  }

  private ListenableFuture<?> getWindowBlockedFuture() {
    // TODO: WindowEngine should return a blocked future here and complete it only when a window
    // event is available, so the driver will be scheduled again after getEvent() can return a
    // non-null event.
    streamSubTask.getWindowEngine();
    return NOT_BLOCKED;
  }

  private boolean isFinishedInternal() {
    checkLockHeld("Lock must be held to call isFinishedInternal");
    boolean finished = state.get() != State.ALIVE || driverContext.isDone();
    if (finished) {
      state.compareAndSet(State.ALIVE, State.NEED_DESTRUCTION);
    }
    return finished;
  }

  @SuppressWarnings({"squid:S1181", "squid:S112"})
  @GuardedBy("exclusiveLock")
  private void destroyIfNecessary() {
    checkLockHeld("Lock must be held to call destroyIfNecessary");
    if (!state.compareAndSet(State.NEED_DESTRUCTION, State.DESTROYED)) {
      return;
    }

    Throwable inFlightException = null;
    try {
      inFlightException = closeAndDestroySubTask();
      driverContext.finished();
    } catch (Throwable t) {
      inFlightException = addSuppressedException(inFlightException, t);
    } finally {
      releaseResource();
    }

    if (inFlightException != null) {
      driverContext.failed(inFlightException);
      throwIfUnchecked(inFlightException);
      throw new RuntimeException(inFlightException);
    }
  }

  @SuppressWarnings("squid:S1181")
  private Throwable closeAndDestroySubTask() {
    boolean wasInterrupted = Thread.interrupted();
    Throwable inFlightException = null;

    driverContext.markTaskClosing();
    // TODO: Close the window engine after WindowEngine exposes a lifecycle close API.

    try {
      streamSubTask.getComputeTask().close();
    } catch (InterruptedException e) {
      wasInterrupted = true;
    } catch (Throwable t) {
      inFlightException = addSuppressedException(inFlightException, t);
    }

    try {
      if (driverContext.mayHaveTmpFile()) {
        cleanTmpFile();
      }
    } catch (Throwable t) {
      inFlightException = addSuppressedException(inFlightException, t);
    }

    try {
      streamSubTask.getSink().stop();
    } catch (Throwable t) {
      inFlightException = addSuppressedException(inFlightException, t);
    } finally {
      if (wasInterrupted) {
        Thread.currentThread().interrupt();
      }
    }

    return inFlightException;
  }

  private void cleanTmpFile() {
    File sortDir =
        new File(
            StreamNodeDescriptor.getInstance().getConfig().getSortTmpDir()
                + File.separator
                + driverContext.getDriverTaskId().getFullId()
                + File.separator
                + driverContext.getPipelineId()
                + File.separator);
    if (!sortDir.exists()) {
      return;
    }
    FileUtils.deleteFileOrDirectory(sortDir, true);
  }

  private void releaseResource() {
    driverContext.getSubTaskContext().signalDriverClosed();
  }

  private static Throwable addSuppressedException(
      Throwable inFlightException, Throwable newException) {
    if (inFlightException == null) {
      return newException;
    }
    if (inFlightException != newException) {
      inFlightException.addSuppressed(newException);
    }
    return inFlightException;
  }

  private <T> Optional<T> tryWithLockUninterruptibly(Supplier<T> task) {
    return tryWithLock(0, TimeUnit.MILLISECONDS, false, task);
  }

  private <T> Optional<T> tryWithLock(
      long timeout, TimeUnit unit, boolean interruptOnClose, Supplier<T> task) {
    checkLockNotHeld("Lock cannot be reacquired");

    boolean acquired = false;
    try {
      acquired = exclusiveLock.tryLock(timeout, unit, interruptOnClose);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    if (!acquired) {
      return Optional.empty();
    }

    T result = null;
    Throwable failure = null;
    try {
      result = task.get();
      destroyIfNecessary();
    } catch (Throwable t) {
      failure = t;
    } finally {
      exclusiveLock.unlock();
    }

    if (state.get() == State.NEED_DESTRUCTION) {
      boolean reacquired = false;
      try {
        reacquired = exclusiveLock.tryLock(false);
        if (reacquired) {
          destroyIfNecessary();
        }
      } catch (Throwable t) {
        if (failure == null) {
          failure = t;
        } else if (failure != t) {
          failure.addSuppressed(t);
        }
      } finally {
        if (reacquired) {
          exclusiveLock.unlock();
        }
      }
    }

    if (failure != null) {
      if (failure instanceof RuntimeException) {
        throw (RuntimeException) failure;
      }
      if (failure instanceof Error) {
        throw (Error) failure;
      }
      throw new RuntimeException(failure);
    }
    verify(result != null, "result is null");
    return Optional.of(result);
  }

  private synchronized void checkLockNotHeld(String message) {
    checkState(!exclusiveLock.isHeldByCurrentThread(), message);
  }

  @GuardedBy("exclusiveLock")
  private synchronized void checkLockHeld(String message) {
    checkState(exclusiveLock.isHeldByCurrentThread(), message);
  }

  private static class DriverLock {
    private final ReentrantLock lock = new ReentrantLock();

    @GuardedBy("this")
    private Thread currentOwner;

    @GuardedBy("this")
    private boolean currentOwnerInterruptionAllowed;

    @GuardedBy("this")
    private List<StackTraceElement> interrupterStack;

    boolean isHeldByCurrentThread() {
      return lock.isHeldByCurrentThread();
    }

    boolean tryLock(boolean currentThreadInterruptionAllowed) {
      if (lock.isHeldByCurrentThread()) {
        throw new IllegalStateException("Lock is not reentrant");
      }
      boolean acquired = lock.tryLock();
      if (acquired) {
        setOwner(currentThreadInterruptionAllowed);
      }
      return acquired;
    }

    boolean tryLock(long timeout, TimeUnit unit, boolean currentThreadInterruptionAllowed)
        throws InterruptedException {
      if (lock.isHeldByCurrentThread()) {
        throw new IllegalStateException("Lock is not reentrant");
      }
      boolean acquired = lock.tryLock(timeout, unit);
      if (acquired) {
        setOwner(currentThreadInterruptionAllowed);
      }
      return acquired;
    }

    private synchronized void setOwner(boolean interruptionAllowed) {
      checkState(lock.isHeldByCurrentThread(), "Current thread does not hold lock");
      currentOwner = Thread.currentThread();
      currentOwnerInterruptionAllowed = interruptionAllowed;
    }

    public synchronized List<StackTraceElement> getInterrupterStack() {
      return interrupterStack;
    }

    public synchronized void interruptCurrentOwner() {
      if (!currentOwnerInterruptionAllowed) {
        return;
      }
      if (interrupterStack == null) {
        interrupterStack = ImmutableList.copyOf(Thread.currentThread().getStackTrace());
      }

      if (currentOwner != null) {
        currentOwner.interrupt();
      }
    }

    public synchronized void unlock() {
      if (!lock.isHeldByCurrentThread()) {
        throw new IllegalStateException("Current thread does not hold lock");
      }
      currentOwner = null;
      currentOwnerInterruptionAllowed = false;
      lock.unlock();
    }
  }
}
