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

import org.apache.iotdb.library.anomaly.util.VARMATrainUtil;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.util.ArrayList;

public class UDTFVARMATrain implements UDTF {

  private VARMATrainUtil trainUtil;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    for (int i = 0; i < validator.getParameters().getAttributes().size(); i++) {
      validator.validateInputSeriesDataType(i, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64);
    }
    // Validate 'p' (AR order) parameter
    if (validator.getParameters().hasAttribute("p")) {
      validator.validate(
          p -> (int) p >= 0, // AR order can be 0 for VMA model, though fit might need p>0
          "Parameter p (AR order) should be a non-negative integer.",
          validator.getParameters().getInt("p"));
    }
    // Validate 'q' (MA order) parameter - new parameter for VARMA
    if (validator.getParameters().hasAttribute("q")) {
      validator.validate(
          q -> (int) q >= 0, // MA order can be 0 for VAR model
          "Parameter q (MA order) should be a non-negative integer.",
          validator.getParameters().getInt("q"));
    }
    if (validator.getParameters().hasAttribute("eta")) {
      validator.validate(
          eta -> (double) eta > 0,
          "Parameter eta should be larger than 0.",
          validator.getParameters().getDouble("eta"));
    }
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy());
    configurations.setOutputDataType(Type.DOUBLE);

    int columnCnt = parameters.getDataTypes().size();
    int p_ar_order = parameters.getIntOrDefault("p", 1); // Default AR order to 1
    int q_ma_order = parameters.getIntOrDefault("q", 0); // Default MA order to 0

    trainUtil = new VARMATrainUtil(columnCnt, p_ar_order, q_ma_order);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (!trainUtil.isNullRow(row)) {
      trainUtil.addRow(row);
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    trainUtil.train(); // This will fit the VARMA model
    ArrayList<Double> coeffs_one_column = trainUtil.coeffsInOneColumn();
    ArrayList<Long> td_time = trainUtil.getTd_time();

    if (!td_time.isEmpty()) {
      long lastTime = td_time.get(td_time.size() - 1);
      for (int i = 0; i < coeffs_one_column.size(); i++) {
        collector.putDouble(
            i < td_time.size() ? td_time.get(i) : lastTime + i - td_time.size() + 1,
            coeffs_one_column.get(i));
      }
    } else if (!coeffs_one_column.isEmpty()) {
      for (int i = 0; i < coeffs_one_column.size(); i++) {
        collector.putDouble(i, coeffs_one_column.get(i));
      }
    }
  }
}
