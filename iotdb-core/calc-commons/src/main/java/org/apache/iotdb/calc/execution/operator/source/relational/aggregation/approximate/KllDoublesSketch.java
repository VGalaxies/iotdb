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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation.approximate;

import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.commons.exception.SemanticException;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;

/**
 * Pure-Java mergeable KLL (Karnin-Lang-Liberty) doubles quantile sketch with bounded normalized
 * rank error {@code O(1 / k)}. Supports lossless associative merges across base slices so multiple
 * coarse-grained windows and distinct target quantiles can share a single compact summary.
 */
public class KllDoublesSketch {

  public static final int DEFAULT_K = 200;
  private static final int MIN_K = 8;
  private static final int MAX_K = 65535;
  private static final int MIN_LEVEL_CAPACITY = 8;
  private static final int SERIALIZATION_VERSION = 1;
  private static final long SEED = 0x9E3779B97F4A7C15L;
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(KllDoublesSketch.class);

  private final int k;
  private long n;
  private double minValue = Double.POSITIVE_INFINITY;
  private double maxValue = Double.NEGATIVE_INFINITY;
  private final List<double[]> levels = new ArrayList<>();
  private int level0Size;
  private SplittableRandom rng = new SplittableRandom(SEED);

  public KllDoublesSketch() {
    this(DEFAULT_K);
  }

  public KllDoublesSketch(int k) {
    if (k < MIN_K || k > MAX_K) {
      throw new SemanticException(
          String.format(
              CalcMessages
                  .EXCEPTION_KLL_SKETCH_PARAMETER_K_MUST_BE_BETWEEN_8_AND_65535_BUT_GOT_ARG_2E5BDC96,
              k));
    }
    this.k = k;
    this.levels.add(new double[levelCapacity(0, 1)]);
    this.level0Size = 0;
  }

  public int getK() {
    return k;
  }

  public long getN() {
    return n;
  }

  public boolean isEmpty() {
    return n == 0L;
  }

  public double getMinValue() {
    return isEmpty() ? Double.NaN : minValue;
  }

  public double getMaxValue() {
    return isEmpty() ? Double.NaN : maxValue;
  }

  public void update(double value) {
    if (!Double.isFinite(value)) {
      return;
    }
    n++;
    if (value < minValue) {
      minValue = value;
    }
    if (value > maxValue) {
      maxValue = value;
    }
    double[] l0 = levels.get(0);
    if (level0Size >= l0.length) {
      int cap0 = levelCapacity(0, levels.size());
      if (l0.length < cap0) {
        l0 = Arrays.copyOf(l0, cap0);
        levels.set(0, l0);
      } else {
        compactLevel(0);
        l0 = levels.get(0);
      }
    }
    l0[level0Size++] = value;
  }

  public void merge(KllDoublesSketch other) {
    if (other == null || other.isEmpty()) {
      return;
    }
    if (other.minValue < this.minValue) {
      this.minValue = other.minValue;
    }
    if (other.maxValue > this.maxValue) {
      this.maxValue = other.maxValue;
    }
    this.n += other.n;

    int otherNumLevels = other.levels.size();
    while (this.levels.size() < otherNumLevels) {
      this.levels.add(new double[0]);
    }
    for (int h = 0; h < otherNumLevels; h++) {
      int otherLen = (h == 0) ? other.level0Size : other.levels.get(h).length;
      if (otherLen == 0) {
        continue;
      }
      if (h == 0) {
        for (int i = 0; i < otherLen; i++) {
          double v = other.levels.get(0)[i];
          double[] l0 = this.levels.get(0);
          if (level0Size >= l0.length) {
            int cap0 = levelCapacity(0, levels.size());
            if (l0.length < cap0) {
              l0 = Arrays.copyOf(l0, cap0);
              levels.set(0, l0);
            } else {
              compactLevel(0);
              l0 = levels.get(0);
            }
          }
          l0[level0Size++] = v;
        }
      } else {
        double[] mine = this.levels.get(h);
        double[] theirs = other.levels.get(h);
        double[] merged = new double[mine.length + theirs.length];
        System.arraycopy(mine, 0, merged, 0, mine.length);
        System.arraycopy(theirs, 0, merged, mine.length, theirs.length);
        Arrays.sort(merged);
        this.levels.set(h, merged);
      }
    }
    compressOverflowingLevels();
  }

  private void compressOverflowingLevels() {
    for (int h = 0; h < levels.size(); h++) {
      int size = (h == 0) ? level0Size : levels.get(h).length;
      int cap = levelCapacity(h, levels.size());
      if (size > cap || (h == 0 && size == cap && levels.size() == 1)) {
        if (size >= cap) {
          compactLevel(h);
        }
      }
    }
  }

  private void compactLevel(int h) {
    double[] src;
    int len;
    if (h == 0) {
      len = level0Size;
      src = Arrays.copyOf(levels.get(0), len);
      level0Size = 0;
    } else {
      src = levels.get(h);
      len = src.length;
      levels.set(h, new double[0]);
    }
    if (len == 0) {
      return;
    }
    Arrays.sort(src, 0, len);
    int offset = rng.nextInt(2);
    int promotedCount = (len - offset + 1) / 2;
    if (promotedCount <= 0) {
      return;
    }
    if (h + 1 >= levels.size()) {
      levels.add(new double[0]);
      // Re-size level 0 capacity gracefully when height expands
      int newCap0 = levelCapacity(0, levels.size());
      if (levels.get(0).length < newCap0) {
        levels.set(0, new double[newCap0]);
      }
    }
    double[] nextExisting = levels.get(h + 1);
    double[] combined = Arrays.copyOf(nextExisting, nextExisting.length + promotedCount);
    int writeIdx = nextExisting.length;
    for (int i = offset; i < len; i += 2) {
      combined[writeIdx++] = src[i];
    }
    Arrays.sort(combined);
    levels.set(h + 1, combined);

    int nextCap = levelCapacity(h + 1, levels.size());
    if (combined.length >= nextCap) {
      compactLevel(h + 1);
    }
  }

  private int levelCapacity(int level, int numLevels) {
    int depthFromTop = Math.max(0, numLevels - 1 - level);
    double scaled = k * Math.pow(2.0 / 3.0, depthFromTop);
    return Math.max(MIN_LEVEL_CAPACITY, (int) Math.ceil(scaled));
  }

  public double getQuantile(double fraction) {
    if (isEmpty() || Double.isNaN(fraction)) {
      return Double.NaN;
    }
    if (fraction <= 0.0) {
      return minValue;
    }
    if (fraction >= 1.0) {
      return maxValue;
    }

    int totalItems = level0Size;
    for (int h = 1; h < levels.size(); h++) {
      totalItems += levels.get(h).length;
    }
    if (totalItems == 0) {
      return Double.NaN;
    }

    double[] values = new double[totalItems];
    long[] weights = new long[totalItems];
    Integer[] order = new Integer[totalItems];
    int idx = 0;
    for (int i = 0; i < level0Size; i++) {
      values[idx] = levels.get(0)[i];
      weights[idx] = 1L;
      order[idx] = idx;
      idx++;
    }
    for (int h = 1; h < levels.size(); h++) {
      double[] arr = levels.get(h);
      long w = 1L << Math.min(60, h);
      for (double v : arr) {
        values[idx] = v;
        weights[idx] = w;
        order[idx] = idx;
        idx++;
      }
    }

    Arrays.sort(order, (a, b) -> Double.compare(values[a], values[b]));
    long totalWeight = 0L;
    for (long w : weights) {
      totalWeight += w;
    }
    double targetWeight = fraction * totalWeight;
    long cum = 0L;
    for (int pos = 0; pos < totalItems; pos++) {
      int itemIdx = order[pos];
      cum += weights[itemIdx];
      if (cum >= targetWeight) {
        return values[itemIdx];
      }
    }
    return maxValue;
  }

  public int computeSerializedSize() {
    int bytes = 4 + 4 + 8 + 8 + 8 + 4; // version, k, n, min, max, numLevels
    bytes += 4 + level0Size * 8;
    for (int h = 1; h < levels.size(); h++) {
      bytes += 4 + levels.get(h).length * 8;
    }
    return bytes;
  }

  public void toByteArray(ByteBuffer buffer) {
    buffer.putInt(SERIALIZATION_VERSION);
    buffer.putInt(k);
    buffer.putLong(n);
    buffer.putDouble(minValue);
    buffer.putDouble(maxValue);
    buffer.putInt(levels.size());
    buffer.putInt(level0Size);
    double[] l0 = levels.get(0);
    for (int i = 0; i < level0Size; i++) {
      buffer.putDouble(l0[i]);
    }
    for (int h = 1; h < levels.size(); h++) {
      double[] arr = levels.get(h);
      buffer.putInt(arr.length);
      for (double v : arr) {
        buffer.putDouble(v);
      }
    }
  }

  public static KllDoublesSketch fromByteBuffer(ByteBuffer buffer) {
    int version = buffer.getInt();
    if (version != SERIALIZATION_VERSION) {
      throw new IllegalStateException(Integer.toString(version));
    }
    int k = buffer.getInt();
    KllDoublesSketch sketch = new KllDoublesSketch(k);
    sketch.n = buffer.getLong();
    sketch.minValue = buffer.getDouble();
    sketch.maxValue = buffer.getDouble();
    int numLevels = buffer.getInt();
    sketch.levels.clear();
    for (int h = 0; h < numLevels; h++) {
      int len = buffer.getInt();
      if (h == 0) {
        int cap0 = Math.max(sketch.levelCapacity(0, numLevels), len);
        double[] arr = new double[cap0];
        for (int i = 0; i < len; i++) {
          arr[i] = buffer.getDouble();
        }
        sketch.levels.add(arr);
        sketch.level0Size = len;
      } else {
        double[] arr = new double[len];
        for (int i = 0; i < len; i++) {
          arr[i] = buffer.getDouble();
        }
        sketch.levels.add(arr);
      }
    }
    if (sketch.levels.isEmpty()) {
      sketch.levels.add(new double[sketch.levelCapacity(0, 1)]);
      sketch.level0Size = 0;
    }
    return sketch;
  }

  public long getEstimatedSize() {
    long size = INSTANCE_SIZE + 64L;
    for (double[] arr : levels) {
      size += RamUsageEstimator.sizeOf(arr);
    }
    return size;
  }

  public void reset() {
    n = 0L;
    minValue = Double.POSITIVE_INFINITY;
    maxValue = Double.NEGATIVE_INFINITY;
    levels.clear();
    levels.add(new double[levelCapacity(0, 1)]);
    level0Size = 0;
    rng = new SplittableRandom(SEED);
  }
}
