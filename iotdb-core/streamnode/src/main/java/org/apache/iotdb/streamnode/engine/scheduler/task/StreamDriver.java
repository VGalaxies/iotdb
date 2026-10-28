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
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;
import org.apache.iotdb.streamnode.engine.window.IEventInfo;
import org.apache.iotdb.streamnode.engine.window.WindowEngine;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.apache.tsfile.read.common.block.TsBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamDriver.class);

  private final StreamDriverContext driverContext;

  private final StreamSubTask streamSubTask;

  private final AtomicLong lastSuccessfulProcessTime = new AtomicLong(0L);

  private final ComputeSlot computeSlot = new ComputeSlot(false);
  private final ComputeSlot recomputeSlot = new ComputeSlot(true);
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
      IStreamSinkTask sinkTask = streamSubTask.getSink();

      if (pendingSinkFuture != null && !pendingSinkFuture.isDone()) {
        return pendingSinkFuture;
      }

      DriverProcessState context = new DriverProcessState();

      // Both slots can contain events for the same window. Incremental compute must finish before
      // full recompute, otherwise a later compute result can overwrite the recomputed result in the
      // shared sink. Prepare both slots first so the comparison is not bypassed by lazy loading.
      prepareEvent(recomputeSlot, context);
      prepareEvent(computeSlot, context);

      if (isSameWindow()) {
        processEvent(sinkTask, computeSlot, context);
        return context.advanced
            ? (pendingSinkFuture.isDone() ? NOT_BLOCKED : pendingSinkFuture)
            : context.toFuture();
      }

      // A destroy-typed recompute head (a clean marker or a dirty full-recompute event) releases
      // the
      // window's source commit once flushed. It must not be dispatched while its window still has
      // compute events queued: isSameWindow above only covers the heads-aligning case, and with a
      // compute backlog plus a destroy order diverging from the close order (per-window MAX_IDLE
      // destroy), the window's close event can still sit in the compute queue when its marker
      // reaches the recompute head. Pushing the marker then lets its EOF flush advance the source
      // commit past the close result that has not been written back yet - a crash in between loses
      // those rows. Defer the head (it stays in its slot) and keep draining the compute slot; it
      // dispatches once the window's queued compute events are gone. The in-flight case needs no
      // extra check: the pass that polls the window's close event always sees both slots holding
      // that window and runs compute first.
      if (recomputeHeadDeferredForQueuedWindowCompute()) {
        processEvent(sinkTask, computeSlot, context);
        return context.advanced
            ? (pendingSinkFuture.isDone() ? NOT_BLOCKED : pendingSinkFuture)
            : context.toFuture();
      }

      // Recompute windows take priority over the current-progress window: a recompute task retains
      // the full window data needed for re-computation, so finishing it first releases that memory
      // immediately instead of keeping it around while the current-progress window runs.
      processEvent(sinkTask, recomputeSlot, context);
      if (context.advanced) {
        return pendingSinkFuture.isDone() ? NOT_BLOCKED : pendingSinkFuture;
      }

      processEvent(sinkTask, computeSlot, context);
      if (context.advanced) {
        return pendingSinkFuture.isDone() ? NOT_BLOCKED : pendingSinkFuture;
      }

      return context.toFuture();
    } catch (Throwable t) {
      Throwable actualCause = t;
      if (actualCause.getCause() instanceof IoTDBRuntimeException) {
        actualCause = actualCause.getCause();
      }

      List<StackTraceElement> interrupterStack = exclusiveLock.getInterrupterStack();
      if (interrupterStack == null) {
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
      throw newException;
    }
  }

  /**
   * Fetches an event without binding or advancing its compute task.
   *
   * <p>{@code bindEventInfo()} is not a pure setter: for a session-scan operator it submits an
   * asynchronous session scan, which starts producing blocks and can occupy the scan executor and
   * exchange memory. Binding a recompute event before it is selected for execution can therefore
   * block the compute task that must finish first. Bind only when the slot is actually dispatched.
   */
  private void prepareEvent(ComputeSlot slot, DriverProcessState context) throws Exception {
    if (slot.event != null) {
      return;
    }

    ListenableFuture<?> blocked = slot.blockedFuture();
    if (!blocked.isDone()) {
      context.addBlockedFuture(blocked);
      return;
    }

    IEventInfo event = slot.fetchEvent();
    if (event == null) {
      return;
    }

    slot.event = event;
  }

  private boolean isSameWindow() {
    if (computeSlot.event == null || recomputeSlot.event == null) {
      return false;
    }

    long computeWindowId = computeSlot.event.getWindowId();
    return computeWindowId >= 0 && computeWindowId == recomputeSlot.event.getWindowId();
  }

  /**
   * Whether the recompute slot's prepared head is a destroy-typed EOF event (a clean {@code
   * WINDOW_DESTROY} marker or a dirty full-recompute event) whose window still has compute events
   * queued in the engine: the head must then stay parked in its slot (see {@link
   * #processInternal}).
   */
  private boolean recomputeHeadDeferredForQueuedWindowCompute() {
    IEventInfo recomputeEvent = recomputeSlot.event;
    if (recomputeEvent == null || !recomputeEvent.isEOF()) {
      return false;
    }
    WindowEngine windowEngine = streamSubTask.getWindowEngine();
    return windowEngine != null
        && windowEngine.hasQueuedComputeEventsForWindow(recomputeEvent.getWindowId());
  }

  /**
   * Advances the prepared event of the given compute slot.
   *
   * <p>The result is recorded into {@code context}: {@code context.advanced} is set to {@code true}
   * only when the task advanced one step (the caller should then return {@code pendingSinkFuture}
   * when the shared sink is backpressured, or {@code NOT_BLOCKED} otherwise); when the task cannot
   * make progress (no event or operator blocked) the relevant blocking future is added to the
   * context for combination and {@code context.advanced} is left {@code false}.
   */
  private void processEvent(IStreamSinkTask sinkTask, ComputeSlot slot, DriverProcessState context)
      throws Exception {
    IEventInfo event = slot.event;
    if (event == null) {
      return;
    }

    if (!event.needsCompute()) {
      // Lifecycle-end marker (e.g. WINDOW_DESTROY): no rows, no compute. Push it so the sink can
      // advance the source commit once the window's earlier entries have flushed.
      pendingSinkFuture = sinkTask.push(null, event.getWindowId(), event.isEOF());
      slot.clear();
      if (pendingSinkFuture.isDone()) {
        lastSuccessfulProcessTime.set(System.currentTimeMillis());
      }
      context.advanced = true;
      return;
    }

    IStreamComputeTask computeTask = slot.computeTask();
    TsBlock result;
    boolean finished = false;
    try {
      slot.bindEventIfNeeded();
      ListenableFuture<?> computeBlocked = computeTask.isBlocked();
      if (!computeBlocked.isDone()) {
        context.addBlockedFuture(computeBlocked);
        return;
      }
      // Reaching this point means this pass drove the compute task one step; the exception path
      // below also counts, so the caller can continue instead of combining blocking futures.
      context.advanced = true;
      result = computeTask.compute();
      finished = computeTask.isFinished();
    } catch (Exception e) {
      // Treat the compute failure (including a failed session scan surfaced by bindEventInfo() or
      // isBlocked()) as an event-level failure: mark the current event, reset the compute task,
      // and drop the event so the driver continues with the next one. Only the affected window's
      // result is dropped; the driver must not abort the whole sub task.
      context.advanced = true;
      event.handleComputeException(e);
      computeTask.reset();
      slot.clear();
      LOGGER.error(
          StreamNodeMessages.LOG_FAILED_TO_COMPUTE_WINDOW_ARG_ARG_ARG_7E4CA205,
          event.getStartTime().orElse(-1L),
          event.getEndTime().orElse(-1L),
          streamSubTask.getStreamName(),
          e);
      return;
    }
    boolean pushed = false;
    if (finished) {
      pendingSinkFuture = sinkTask.push(result, event.getWindowId(), event.isEOF());
      computeTask.reset();
      slot.clear();
      pushed = true;
    } else if (result != null && !result.isEmpty()) {
      // Intermediate result for the same window; not EOF.
      pendingSinkFuture = sinkTask.push(result, event.getWindowId(), false);
      pushed = true;
    }

    if (pushed && pendingSinkFuture.isDone()) {
      lastSuccessfulProcessTime.set(System.currentTimeMillis());
    }
  }

  /**
   * Represents one of the two compute paths (current-progress or recompute): the bound event and
   * the dispatch to the corresponding compute task and window event source. The {@code isRecompute}
   * flag selects which path, keeping the driver logic free of branching.
   */
  private class ComputeSlot {
    private final boolean isRecompute;
    private IEventInfo event;
    private boolean bound;

    private ComputeSlot(boolean isRecompute) {
      this.isRecompute = isRecompute;
    }

    private IStreamComputeTask computeTask() {
      return isRecompute ? streamSubTask.getRecomputeTask() : streamSubTask.getComputeTask();
    }

    private IEventInfo fetchEvent() {
      WindowEngine windowEngine = streamSubTask.getWindowEngine();
      if (windowEngine == null) {
        return null;
      }
      return isRecompute ? windowEngine.pollRecomputeEvent() : windowEngine.pollComputeEvent();
    }

    private ListenableFuture<?> blockedFuture() {
      WindowEngine windowEngine = streamSubTask.getWindowEngine();
      if (windowEngine == null) {
        return NOT_BLOCKED;
      }
      return isRecompute
          ? windowEngine.getRecomputeBlockedFuture()
          : windowEngine.getBlockedFuture();
    }

    private void bindEventIfNeeded() throws Exception {
      if (!bound) {
        computeTask().bindEventInfo(event);
        bound = true;
      }
    }

    private void clear() {
      if (event != null) {
        // The event is dropped without a guarantee that its rows were fully consumed (e.g. a plan
        // without ${rows} has no EventScanOperator, so nothing ever drains the iterator): release
        // it so its iterator unpins the window's row store.
        event.release();
      }
      event = null;
      bound = false;
    }
  }

  /**
   * Carries the per-pass state of a single {@link #processInternal()} invocation: whether the last
   * attempted task advanced one step, and the window / compute blocking futures gathered along the
   * way. The shared sink backpressure is handled separately via {@code pendingSinkFuture}, so it
   * never participates in the combination here.
   *
   * <p>Recompute and current-progress are independent, so their window / compute blocking futures
   * may be combined: whichever completes first wakes the driver, allowing one computation to keep
   * running while the other is stalled (no event or operator blocked).
   */
  private static class DriverProcessState {
    /** Whether the most recently attempted task advanced one step. */
    boolean advanced;

    private final List<ListenableFuture<?>> blockedFutures = new ArrayList<>(2);

    void addBlockedFuture(ListenableFuture<?> future) {
      blockedFutures.add(future);
    }

    ListenableFuture<?> toFuture() {
      if (blockedFutures.isEmpty()) {
        return NOT_BLOCKED;
      }
      if (blockedFutures.size() == 1) {
        return blockedFutures.get(0);
      }
      SettableFuture<Void> combined = SettableFuture.create();
      for (ListenableFuture<?> future : blockedFutures) {
        future.addListener(
            () -> {
              if (!combined.isDone()) {
                combined.set(null);
              }
            },
            directExecutor());
      }
      return combined;
    }
  }

  @Override
  public ListenableFuture<?> push(List<DataSlice> dataSlices) {
    WindowEngine windowEngine = streamSubTask.getWindowEngine();
    if (windowEngine != null && dataSlices != null && !dataSlices.isEmpty()) {
      try {
        return windowEngine.push(dataSlices);
      } catch (UnsupportedOperationException e) {
        // The window engine does not accept data (e.g. period windows are time-driven and fetch
        // data downstream by time range); silently ignore the pushed batch.
        return NOT_BLOCKED;
      }
    }
    return NOT_BLOCKED;
  }

  @Override
  public long getLastSuccessfulProcessTime() {
    return lastSuccessfulProcessTime.get();
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

    try {
      streamSubTask.getWindowEngine().forceClose();
    } catch (Throwable t) {
      inFlightException = addSuppressedException(inFlightException, t);
    }

    // forceClose() just emitted the windows' final events into the engine's queues, but this
    // driver is being destroyed and will never consume them: release them (and any event still
    // held by a compute slot) so their iterators unpin the windows' row stores.
    releaseUnconsumedEvents();
    computeSlot.clear();
    recomputeSlot.clear();

    try {
      streamSubTask.getComputeTask().close();
    } catch (InterruptedException e) {
      wasInterrupted = true;
    } catch (Throwable t) {
      inFlightException = addSuppressedException(inFlightException, t);
    }

    try {
      streamSubTask.getRecomputeTask().close();
    } catch (InterruptedException e) {
      wasInterrupted = true;
    } catch (Throwable t) {
      inFlightException = addSuppressedException(inFlightException, t);
    }

    try {
      if (streamSubTask.getComputeTask().getStreamComputeTaskContext().mayHaveTmpFile()
          || streamSubTask.getRecomputeTask().getStreamComputeTaskContext().mayHaveTmpFile()) {
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

  /**
   * Drains both window-event queues of the subtask's engine and releases every event: on driver
   * destruction nobody consumes them anymore, and an unreleased event's iterator would pin its
   * window's row store (leaking the store's data-pool charge).
   */
  private void releaseUnconsumedEvents() {
    WindowEngine windowEngine = streamSubTask.getWindowEngine();
    if (windowEngine == null) {
      return;
    }
    IEventInfo event;
    while ((event = windowEngine.pollComputeEvent()) != null) {
      event.release();
    }
    while ((event = windowEngine.pollRecomputeEvent()) != null) {
      event.release();
    }
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
