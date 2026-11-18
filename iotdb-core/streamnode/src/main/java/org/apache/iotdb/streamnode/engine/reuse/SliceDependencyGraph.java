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

package org.apache.iotdb.streamnode.engine.reuse;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Thread-safe acyclic directed dependency graph between base-slice producers and coarse-window
 * consumers, supporting multi-hop reuse chains (e.g. {@code 1s -> 10s -> 1m}).
 */
public class SliceDependencyGraph {

  private final Map<String, Set<String>> producerToConsumers = new HashMap<>();
  private final Map<String, String> consumerToProducer = new HashMap<>();

  public synchronized boolean bindEdge(String producerStream, String consumerStream) {
    if (producerStream == null || consumerStream == null || producerStream.equals(consumerStream)) {
      return false;
    }
    if (wouldCreateCycle(producerStream, consumerStream)) {
      return false;
    }
    String existingProducer = consumerToProducer.get(consumerStream);
    if (existingProducer != null && !existingProducer.equals(producerStream)) {
      Set<String> prevConsumers = producerToConsumers.get(existingProducer);
      if (prevConsumers != null) {
        prevConsumers.remove(consumerStream);
      }
    }
    consumerToProducer.put(consumerStream, producerStream);
    producerToConsumers
        .computeIfAbsent(producerStream, k -> new LinkedHashSet<>())
        .add(consumerStream);
    return true;
  }

  public synchronized void removeStream(String streamName) {
    String boundProducer = consumerToProducer.remove(streamName);
    if (boundProducer != null) {
      Set<String> consumers = producerToConsumers.get(boundProducer);
      if (consumers != null) {
        consumers.remove(streamName);
      }
    }
    Set<String> downstream = producerToConsumers.remove(streamName);
    if (downstream != null) {
      for (String consumer : downstream) {
        consumerToProducer.remove(consumer);
      }
    }
  }

  public synchronized String getBoundProducer(String consumerStream) {
    return consumerToProducer.get(consumerStream);
  }

  public synchronized Set<String> getConsumers(String producerStream) {
    Set<String> consumers = producerToConsumers.get(producerStream);
    return consumers == null
        ? Collections.emptySet()
        : Collections.unmodifiableSet(new LinkedHashSet<>(consumers));
  }

  public synchronized boolean isActiveProducer(String producerStream) {
    Set<String> consumers = producerToConsumers.get(producerStream);
    return consumers != null && !consumers.isEmpty();
  }

  private boolean wouldCreateCycle(String producerStream, String consumerStream) {
    Deque<String> queue = new ArrayDeque<>();
    Set<String> visited = new HashSet<>();
    queue.add(consumerStream);
    while (!queue.isEmpty()) {
      String curr = queue.poll();
      if (curr.equals(producerStream)) {
        return true;
      }
      if (!visited.add(curr)) {
        continue;
      }
      Set<String> nexts = producerToConsumers.get(curr);
      if (nexts != null) {
        queue.addAll(nexts);
      }
    }
    return false;
  }
}
