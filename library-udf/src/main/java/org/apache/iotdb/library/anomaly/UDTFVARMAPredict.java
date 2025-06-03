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

package org.apache.iotdb.library.anomaly;

import org.apache.iotdb.library.anomaly.util.VARMA;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.util.ArrayList;

public class UDTFVARMAPredict implements UDTF {

  // Number of original series (k) is inferred as (inputSeries.size() - 1).
  private int k;
  private int p_ar; // AR order
  private int q_ma; // MA order
  private int targetIndex; // Which column to output (0..k-1)
  private int horizon; // How many future points to predict

  // Buffers for original td and their timestamps
  private final ArrayList<ArrayList<Double>> td = new ArrayList<>();
  private final ArrayList<Long> tdTime = new ArrayList<>();

  // Buffer for the flattened coefficient vector (length = k + p·k² + q·k²)
  private final ArrayList<Double> modelCoeffs = new ArrayList<>();

  // We also need to remember the last two timestamps to compute the prediction interval:
  private Long lastTimestamp = null;
  private Long secondLastTimestamp = null;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    // All input series must be numeric (DOUBLE/FLOAT/INT32/INT64)
    for (int i = 0; i < validator.getParameters().getAttributes().size(); i++) {
      validator.validateInputSeriesDataType(i, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64);
    }

    // AR order 'p'
    if (validator.getParameters().hasAttribute("p")) {
      validator.validate(
          p -> (int) p >= 0,
          "Parameter p (AR order) must be a non-negative integer.",
          validator.getParameters().getInt("p"));
    }

    // MA order 'q'
    if (validator.getParameters().hasAttribute("q")) {
      validator.validate(
          q -> (int) q >= 0,
          "Parameter q (MA order) must be a non-negative integer.",
          validator.getParameters().getInt("q"));
    }

    // Target column 'col'
    if (!validator.getParameters().hasAttribute("col")) {
      throw new IllegalArgumentException(
          "Missing parameter 'col': which original series index to forecast (0..k-1).");
    }
    // We cannot validate its range yet (k unknown until beforeStart), but ensure it's non-negative:
    validator.validate(
        col -> (int) col >= 0,
        "Parameter col must be a non-negative integer.",
        validator.getParameters().getInt("col"));

    // Forecast horizon
    if (!validator.getParameters().hasAttribute("horizon")) {
      throw new IllegalArgumentException(
          "Missing parameter 'horizon': number of future points to forecast.");
    }
    validator.validate(
        h -> (int) h > 0,
        "Parameter horizon must be a positive integer.",
        validator.getParameters().getInt("horizon"));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    // We will read all rows first (buffer), so do RowByRow access:
    configurations.setAccessStrategy(new RowByRowAccessStrategy());
    configurations.setOutputDataType(Type.DOUBLE);

    int totalSeries = parameters.getChildExpressionsSize();
    // The last series is the coefficient vector, so k = totalSeries - 1:
    this.k = totalSeries - 1;

    // Read AR/MA orders
    this.p_ar = parameters.getIntOrDefault("p", 1);
    this.q_ma = parameters.getIntOrDefault("q", 0);

    // Read which column (0..k-1) to output
    this.targetIndex = parameters.getInt("col");
    if (this.targetIndex < 0 || this.targetIndex >= this.k) {
      throw new IllegalArgumentException(
          "Parameter col=" + this.targetIndex + " is out of range [0.." + (this.k - 1) + "]");
    }

    // Number of time points to forecast
    this.horizon = parameters.getInt("horizon");

    // Initialize buffers
    td.clear();
    tdTime.clear();
    modelCoeffs.clear();
    lastTimestamp = null;
    secondLastTimestamp = null;
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    // The row contains: [series0, series1, …, series_{k-1}, modelSeries]
    // 1) If any of series0..series_{k-1} is non-null, buffer that entire vector + timestamp
    boolean hasTdValue = false;
    ArrayList<Double> currentTd = new ArrayList<>(k);
    for (int i = 0; i < k; i++) {
      if (!row.isNull(i)) {
        hasTdValue = true;
        double v = Util.getValueAsDouble(row, i);
        currentTd.add(v);
      } else {
        currentTd.add(null);
      }
    }
    if (hasTdValue) {
      // Fill nulls in this row's vector with the "last seen" approach:
      for (int i = 0; i < k; i++) {
        if (currentTd.get(i) == null) {
          // look back to the previous non-null in the same column
          Double last = null;
          for (int t = td.size() - 1; t >= 0; t--) {
            if (td.get(t).get(i) != null) {
              last = td.get(t).get(i);
              break;
            }
          }
          currentTd.set(i, last == null ? 0.0 : last);
        }
      }
      td.add(currentTd);
      long tstamp = row.getTime();
      tdTime.add(tstamp);

      // Track the last two timestamps to compute a constant interval
      if (lastTimestamp != null) {
        secondLastTimestamp = lastTimestamp;
      }
      lastTimestamp = tstamp;
    }

    // 2) If the final column (index = k) is non-null, buffer it as part of modelCoeffs
    if (!row.isNull(k)) {
      double coeffVal = Util.getValueAsDouble(row, k);
      modelCoeffs.add(coeffVal);
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    // If we have no data, just return:
    if (td.isEmpty() || modelCoeffs.isEmpty()) {
      // Nothing to predict
      return;
    }

    // Build the VARMA model by reconstructing from modelCoeffs:
    //   expected length = k + (p·k²) + (q·k²)
    int expectedSize = k + (p_ar * k * k) + (q_ma * k * k);
    if (modelCoeffs.size() != expectedSize) {
      throw new IllegalArgumentException(
          "Length of modelCoeffs ("
              + modelCoeffs.size()
              + ") does not match expected size ("
              + expectedSize
              + ").");
    }

    // 1) Recreate VARMA instance and load coefficients
    VARMA varmaModel = new VARMA(k, p_ar, q_ma);
    varmaModel.fitCoeffs(modelCoeffs, k, p_ar, q_ma);

    // 2) Build the initial “window” for forecasting: the LAST p observed vectors in 'td'
    ArrayList<ArrayList<Double>> window = new ArrayList<>(p_ar);
    if (p_ar > 0) {
      int startIdx = Math.max(0, td.size() - p_ar);
      for (int i = startIdx; i < td.size(); i++) {
        window.add(new ArrayList<>(td.get(i))); // deep copy
      }
      // If we had fewer than p_ar observations (rare), we’d just pad with zeros
      while (window.size() < p_ar) {
        ArrayList<Double> zeroVec = new ArrayList<>(k);
        for (int i = 0; i < k; i++) {
          zeroVec.add(0.0);
        }
        window.add(0, zeroVec);
      }
    }

    // 3) Determine the fixed time‐interval to step forward: assume constant = last - secondLast
    long interval;
    if (lastTimestamp != null && secondLastTimestamp != null) {
      interval = lastTimestamp - secondLastTimestamp;
      if (interval <= 0) {
        interval = 1; // Fallback if timestamps were identical or invalid
      }
    } else {
      interval = 1; // Default to 1 if we only saw a single timestamp
    }

    // 4) Iteratively forecast 'horizon' steps ahead
    long currentTime = lastTimestamp;
    for (int h = 0; h < horizon; h++) {
      // Predict one step ahead:
      ArrayList<Double> nextVec = varmaModel.predict(window);

      // Emit only the requested column:
      double predictedValue = nextVec.get(targetIndex);
      long nextTime = currentTime + interval;
      collector.putDouble(nextTime, predictedValue);

      // Slide the window:
      if (p_ar > 0) {
        if (window.size() == p_ar) {
          window.remove(0);
        }
        window.add(nextVec);
      }

      currentTime = nextTime;
    }
  }
}
