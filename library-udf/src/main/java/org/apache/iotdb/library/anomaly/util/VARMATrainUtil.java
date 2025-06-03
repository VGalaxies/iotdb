package org.apache.iotdb.library.anomaly.util;

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.access.Row;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;

public class VARMATrainUtil {
  private final ArrayList<ArrayList<Double>> td = new ArrayList<>();
  private final ArrayList<ArrayList<Double>> md = new ArrayList<>();
  private final ArrayList<Long> td_time = new ArrayList<>();

  private final int columnCnt;
  private final int p_ar; // AR order for VARMA
  private final int q_ma; // MA order for VARMA
  private double eta;

  private double[] std;
  private KDTreeUtil kdTreeUtil;
  private VARMA prediction_model; // Changed from VAR/ARMA to VARMA

  // Updated constructor to include q_ma
  public VARMATrainUtil(int columnCnt, int p_ar, int q_ma, double eta) {
    this.columnCnt = columnCnt;
    this.p_ar = p_ar;
    this.q_ma = q_ma;
    this.eta = eta;
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

    ArrayList<Double> mt = new ArrayList<>();
    boolean containsNotNullMd = false;
    for (int i = this.columnCnt; i < row.size(); i++) { // Assuming md follows td
      if (!row.isNull(i)) {
        containsNotNullMd = true;
        BigDecimal bd = BigDecimal.valueOf(Util.getValueAsDouble(row, i));
        mt.add(bd.doubleValue());
      } else {
        mt.add(null);
      }
    }
    if (containsNotNullMd) {
      md.add(mt);
    }
  }

  public void buildKDTree() {
    if (this.md.isEmpty()) {
      throw new IllegalStateException("Master data (md) is empty, cannot build KDTree.");
    }
    // Pre-process md for nulls if KDTreeUtil cannot handle them.
    // For simplicity, assume KDTreeUtil can handle or md is clean.
    // A simple strategy: replace nulls with mean of column or 0.
    ArrayList<ArrayList<Double>> processed_md = new ArrayList<>();
    for (ArrayList<Double> row : this.md) {
      ArrayList<Double> newRow = new ArrayList<>();
      for (Double val : row) {
        newRow.add(val == null ? 0.0 : val); // Example: replace null with 0
      }
      processed_md.add(newRow);
    }
    this.kdTreeUtil = new KDTreeUtil();
    this.kdTreeUtil.buildTree(processed_md);
  }

  public double delta(ArrayList<Double> t_tuple, ArrayList<Double> m_tuple) {
    if (t_tuple == null
        || m_tuple == null
        || t_tuple.size() != m_tuple.size()
        || t_tuple.size() != columnCnt) {
      return Double.MAX_VALUE;
    }
    double distance = 0d;
    for (int pos = 0; pos < columnCnt; pos++) {
      Double val_t = t_tuple.get(pos);
      Double val_m = m_tuple.get(pos);
      if (val_t == null || val_m == null) continue; // Skip if any part is null
      if (std[pos] == 0) continue;
      double temp = val_t - val_m;
      temp = temp / std[pos];
      distance += temp * temp;
    }
    return Math.sqrt(distance);
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

  private double varianceImperative(double[] value) {
    if (value.length == 0) return 0.0;
    double average = 0.0;
    for (double v : value) average += v;
    average /= value.length;
    double variance = 0.0;
    for (double v : value) variance += (v - average) * (v - average);
    return value.length > 1 ? variance / (value.length - 1) : variance / value.length;
  }

  private double[] getColumn(int pos) {
    if (td.isEmpty()) return new double[0];
    double[] column = new double[this.td.size()];
    for (int i = 0; i < this.td.size(); i++) {
      column[i] = this.td.get(i).get(pos);
    }
    return column;
  }

  public void call_std() {
    this.std = new double[this.columnCnt];
    for (int i = 0; i < this.columnCnt; i++) {
      std[i] = Math.sqrt(varianceImperative(getColumn(i)));
      if (std[i] == 0) std[i] = 1.0; // Avoid division by zero
    }
  }

  public boolean checkConsistency(ArrayList<Double> tuple) {
    if (kdTreeUtil == null) {
      throw new IllegalStateException("KDTree not built.");
    }
    ArrayList<Double> NN = kdTreeUtil.findTheNearestNeighbor(tuple);
    double d = delta(tuple, NN);
    return d <= eta;
  }

  public void getOriginalAnomaliesAndTrainModel() {
    // The logic for learning_samples based on 'p_ar' consecutive normal points
    // This 'p_ar' from constructor is the AR order for VARMA.
    // The original code used 'p' for window length of p+1.
    // Let's assume this 'p_ar' refers to the VARMA AR order.
    // A sequence of normal points is needed to train. The length of this sequence
    // should be greater than p_ar to allow for lagged observations.

    if (td.size() <= this.p_ar && this.p_ar > 0) {
      System.err.println(
          "Not enough data in 'td' to train VARMA model. Need more than 'p_ar' ("
              + this.p_ar
              + ") data points. Found: "
              + td.size());
      this.prediction_model = new VARMA(columnCnt, this.p_ar, this.q_ma);
      // Fit with empty or minimal data will likely result in zero coefficients by VARMA.fit().
      if (!td.isEmpty()) {
        try {
          this.prediction_model.fit(new ArrayList<>(td.subList(0, Math.min(td.size(), td.size()))));
        } // Try to fit
        catch (IllegalArgumentException e) {
          System.err.println("Error during fitting with insufficient data: " + e.getMessage());
        }
      } else {
        this.prediction_model.fit(new ArrayList<>()); // Fit with empty, VARMA.fit handles it
      }
      return;
    }

    // The original logic for 'learning_samples' used 'p' as a segment length.
    // If p_ar is VARMA order, we need at least p_ar+1 points for one regression step.
    // Let's collect all segments of data deemed "normal".
    ArrayList<ArrayList<Double>> learning_samples = new ArrayList<>();
    int consecutiveNormalStart = -1;
    for (int i = 0; i < this.td.size(); i++) {
      ArrayList<Double> tuple = this.td.get(i);
      boolean isNormal = checkConsistency(tuple);

      if (isNormal) {
        if (consecutiveNormalStart == -1) {
          consecutiveNormalStart = i;
        }
      } else {
        if (consecutiveNormalStart != -1) {
          // Add the segment of normal data if long enough
          // A segment needs to be at least p_ar + 1 for VARMA(p_ar, q_ma) fitting using OLS for AR
          // part.
          if (i - consecutiveNormalStart > this.p_ar
              || this.p_ar == 0) { // p_ar=0 case, any length is fine
            for (int j = consecutiveNormalStart; j < i; j++) {
              learning_samples.add(this.td.get(j));
            }
          }
          consecutiveNormalStart = -1;
        }
      }
    }
    // Add any trailing normal segment
    if (consecutiveNormalStart != -1) {
      if (this.td.size() - consecutiveNormalStart > this.p_ar || this.p_ar == 0) {
        for (int j = consecutiveNormalStart; j < this.td.size(); j++) {
          learning_samples.add(this.td.get(j));
        }
      }
    }

    this.prediction_model = new VARMA(columnCnt, this.p_ar, this.q_ma);
    if (learning_samples.size() > this.p_ar || (this.p_ar == 0 && !learning_samples.isEmpty())) {
      this.prediction_model.fit(learning_samples);
    } else {
      System.err.println(
          "Warning: Not enough 'normal' learning samples ("
              + learning_samples.size()
              + ") collected to reliably fit VARMA model of AR order "
              + this.p_ar
              + ". Model coefficients might be trivial.");
      if (!learning_samples.isEmpty()) { // Try to fit with what's available
        try {
          this.prediction_model.fit(learning_samples);
        } catch (IllegalArgumentException e) {
          System.err.println("Error fitting VARMA with few learning samples: " + e.getMessage());
        }
      } else { // If learning_samples is empty, fit with empty list
        this.prediction_model.fit(new ArrayList<>()); // VARMA.fit should handle this
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

    if (md.isEmpty()) {
      System.err.println(
          "Master data (md) is empty. KDTree based consistency checks skipped. Training VARMA on all available td data.");
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
      return;
    }

    buildKDTree();
    call_std();
    getOriginalAnomaliesAndTrainModel();
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

  public ArrayList<ArrayList<Double>> getMd() {
    return md;
  }

  public ArrayList<ArrayList<Double>> getTd() {
    return td;
  }

  public ArrayList<Long> getTd_time() {
    return td_time;
  }
}
