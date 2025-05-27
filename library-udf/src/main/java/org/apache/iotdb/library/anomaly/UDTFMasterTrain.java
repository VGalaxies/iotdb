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

import org.apache.iotdb.library.anomaly.util.MasterTrainUtil;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.util.ArrayList;

public class UDTFMasterTrain implements UDTF {

  private MasterTrainUtil masterTrainUtil;

  private int columnCnt;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    for (int i = 0; i < validator.getParameters().getAttributes().size(); i++) {
      validator.validateInputSeriesDataType(i, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64);
    }
    if (validator.getParameters().hasAttribute("p")) {
      validator.validate(
          p -> (int) p > 0,
          "Order p should be a positive integer.",
          validator.getParameters().getInt("p"));
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
    columnCnt = parameters.getDataTypes().size() / 2;
    int p = parameters.getIntOrDefault("p", -1);
    double eta = parameters.getDoubleOrDefault("eta", 1.0);
    masterTrainUtil = new MasterTrainUtil(columnCnt, p, eta);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (!masterTrainUtil.isNullRow(row)) {
      masterTrainUtil.addRow(row);
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    masterTrainUtil.train();
    ArrayList<Double> coeffs_one_column = masterTrainUtil.coeffsInOneColumn();
    ArrayList<Long> td_time = masterTrainUtil.getTd_time();
    for (int i = 0; i < coeffs_one_column.size(); i++) {
      collector.putDouble(td_time.get(i), coeffs_one_column.get(i));
    }
  }
}
