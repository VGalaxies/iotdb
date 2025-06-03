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

package org.apache.iotdb.library.anomaly.util;

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.access.Row;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;

public class VARMATrainUtil {
  private final ArrayList<ArrayList<Double>> td = new ArrayList<>();
  private final ArrayList<Long> td_time = new ArrayList<>();

  private final int columnCnt;
  private final int p_ar; // AR order for VARMA
  private final int q_ma; // MA order for VARMA
  private VARMA prediction_model;

  // Updated constructor to include q_ma
  public VARMATrainUtil(int columnCnt, int p_ar, int q_ma) {
    this.columnCnt = columnCnt;
    this.p_ar = p_ar;
    this.q_ma = q_ma;
  }

  public boolean isNullRow(Row row) throws IOException {
    boolean flag = true;
    for (int i = 0; i < row.size(); i++) {
      if (!row.isNull(i)) {
        flag = false;
        break;
      }
    }
    return flag;
  }

  public void addRow(Row row) throws Exception {
    ArrayList<Double> tt = new ArrayList<>();
    boolean containsNotNullTd = false;
    for (int i = 0; i < this.columnCnt; i++) {
      if (!row.isNull(i)) {
        containsNotNullTd = true;
        BigDecimal bd = BigDecimal.valueOf(Util.getValueAsDouble(row, i));
        tt.add(bd.doubleValue());
      } else {
        tt.add(null);
      }
    }
    if (containsNotNullTd) {
      td.add(tt);
      td_time.add(row.getTime());
    }
  }

  public void fillNullValue() { // For td
    if (td.isEmpty()) return;
    for (int i = 0; i < columnCnt; i++) {
      Double lastSeenValue = null;
      for (ArrayList<Double> arrayList : this.td) {
        if (arrayList.get(i) != null) {
          lastSeenValue = arrayList.get(i);
          break;
        }
      }
      if (lastSeenValue == null && !td.isEmpty()) {
        lastSeenValue = 0.0;
      }
      for (ArrayList<Double> arrayList : this.td) {
        if (arrayList.get(i) == null) {
          arrayList.set(i, lastSeenValue);
        } else {
          lastSeenValue = arrayList.get(i);
        }
      }
    }
  }

  public void train() {
    if (td.isEmpty()) {
      System.err.println("Time series data (td) is empty. Cannot train model.");
      this.prediction_model = new VARMA(columnCnt, this.p_ar, this.q_ma);
      this.prediction_model.fit(new ArrayList<>()); // Initialize with empty data
      return;
    }
    fillNullValue(); // Fill nulls in td

    this.prediction_model = new VARMA(columnCnt, this.p_ar, this.q_ma);
    if (td.size() > this.p_ar
        || (this.p_ar == 0 && !td.isEmpty())) { // Need enough data for AR part
      this.prediction_model.fit(this.td);
    } else {
      System.err.println(
          "Warning: Not enough td data ("
              + td.size()
              + ") to train VARMA model (AR order "
              + this.p_ar
              + ") when md is empty.");
      if (!td.isEmpty()) {
        try {
          this.prediction_model.fit(this.td);
        } // Try with what's there
        catch (IllegalArgumentException e) {
          System.err.println("Error fitting VARMA with insufficient td data: " + e.getMessage());
        }
      } else {
        this.prediction_model.fit(new ArrayList<>());
      }
    }
  }

  public ArrayList<Double> coeffsInOneColumn() {
    if (prediction_model == null) {
      System.err.println("Prediction model not initialized before calling coeffsInOneColumn.");
      // Return empty or appropriately sized zero list for safety
      int k = this.columnCnt;
      int p = this.p_ar;
      int q = this.q_ma;
      int expectedSize = k + (p * k * k) + (q * k * k);
      return new ArrayList<>(Collections.nCopies(expectedSize, 0.0));
    }
    return prediction_model.getCoeffsInOneColumn();
  }

  public ArrayList<ArrayList<Double>> getTd() {
    return td;
  }

  public ArrayList<Long> getTd_time() {
    return td_time;
  }
}
