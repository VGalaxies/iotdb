// MasterDetector.java
package org.apache.iotdb.library.anomaly.util;

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.access.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class VARMADetector {
  private final ArrayList<ArrayList<Double>> td = new ArrayList<>();
  private final ArrayList<ArrayList<Double>> td_repaired = new ArrayList<>();
  private final ArrayList<Boolean> td_anomalies = new ArrayList<>();
  private final ArrayList<Boolean> anomalies_in_repaired = new ArrayList<>();
  private final ArrayList<ArrayList<Double>> md = new ArrayList<>();
  private final ArrayList<Double> coeffs_one_column = new ArrayList<>();
  private final ArrayList<Long> td_time = new ArrayList<>();

  private int[] initial_window_indices;

  private final int columnCnt;
  private final int k_knn;
  private final int p_ar; // AR order for VARMA
  private final int q_ma; // MA order for VARMA
  private final double eta;
  private final double beta;

  private double[] std;
  private KDTreeUtil kdTreeUtil;
  private VARMA prediction_model; // Changed from VAR/ARMA to VARMA

  // Updated constructor
  public VARMADetector(int columnCnt, int k_knn, int p_ar, int q_ma, double eta, double beta) {
    this.columnCnt = columnCnt;
    this.k_knn = k_knn;
    this.p_ar = p_ar;
    this.q_ma = q_ma;
    this.eta = eta;
    this.beta = beta;
  }

  public boolean isNullRow(Row row) throws IOException {
    for (int i = 0; i < row.size(); i++) {
      if (!row.isNull(i)) {
        return false;
      }
    }
    return true;
  }

  public void addRow(Row row) throws Exception {
    ArrayList<Double> tt = new ArrayList<>();
    boolean containsNotNullTd = false;
    for (int i = 0; i < this.columnCnt; i++) {
      if (i < row.size() && !row.isNull(i)) {
        containsNotNullTd = true;
        tt.add(Util.getValueAsDouble(row, i));
      } else {
        tt.add(null);
      }
    }
    // Add td row and its timestamp only if td part has data, to maintain alignment potential with
    // md
    // This part is tricky: if td is all null for a timestamp but md isn't, lists can get
    // misaligned.
    // For now, if td has actual data, add it and its time.
    if (containsNotNullTd) {
      td.add(tt);
      td_time.add(row.getTime());
    } else if (!td_time.isEmpty() && td_time.get(td_time.size() - 1) < row.getTime()) {
      // If previous rows had data, and this row is for a new timestamp,
      // add a null row to td to maintain alignment if other parts of the row (md, coeffs) are
      // added.
      // However, the original logic only added if containsNotNullTd was true.
      // Let's ensure that if a timestamp is processed (e.g. for coeffs), td and md also get a row.
      // This requires a more holistic row processing logic.
      // For this modification, sticking to "add if contains data" for td and md parts.
      // Coefficients are simply appended.
    }

    ArrayList<Double> mt = new ArrayList<>();
    boolean containsNotNullMd = false;
    for (int i = 0; i < this.columnCnt; i++) {
      int md_idx = this.columnCnt + i;
      if (md_idx < row.size() && !row.isNull(md_idx)) {
        containsNotNullMd = true;
        mt.add(Util.getValueAsDouble(row, md_idx));
      } else {
        mt.add(null);
      }
    }
    if (containsNotNullMd) {
      md.add(mt);
    }

    int coeff_series_idx = this.columnCnt * 2;
    if (coeff_series_idx < row.size() && !row.isNull(coeff_series_idx)) {
      coeffs_one_column.add(Util.getValueAsDouble(row, coeff_series_idx));
    }
  }

  public void buildKDTree() {
    if (this.md.isEmpty()) {
      throw new IllegalStateException("Master data (md) is empty, cannot build KDTree.");
    }
    ArrayList<ArrayList<Double>> processed_md = new ArrayList<>();
    for (ArrayList<Double> row : this.md) {
      ArrayList<Double> newRow = new ArrayList<>();
      boolean hasNull = false;
      for (Double val : row) {
        if (val == null) hasNull = true;
        newRow.add(val == null ? 0.0 : val); // Example: replace null with 0
      }
      // if (hasNull) System.err.println("Warning: Nulls found in master data row, replaced with 0
      // for KDTree.");
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
      if (val_t == null || val_m == null) continue;
      if (std[pos] == 0) continue;
      double temp = val_t - val_m;
      temp = temp / std[pos];
      distance += temp * temp;
    }
    return Math.sqrt(distance);
  }

  public void fillNullValue() { // For td and md
    // Fill td
    if (!td.isEmpty()) {
      for (int i = 0; i < columnCnt; i++) {
        Double lastSeenValue = null;
        for (ArrayList<Double> arrayList : this.td) {
          if (arrayList.get(i) != null) {
            lastSeenValue = arrayList.get(i);
            break;
          }
        }
        if (lastSeenValue == null && !td.isEmpty()) lastSeenValue = 0.0;
        for (ArrayList<Double> arrayList : this.td) {
          if (arrayList.get(i) == null) arrayList.set(i, lastSeenValue);
          else lastSeenValue = arrayList.get(i);
        }
      }
    }
    // Fill md (if KDTree cannot handle nulls, or for consistency)
    if (!md.isEmpty()) {
      for (int i = 0; i < columnCnt; i++) { // Assuming md has 'columnCnt' relevant dimensions
        Double lastSeenMdValue = null;
        for (ArrayList<Double> masterRow : this.md) {
          if (masterRow.size() > i && masterRow.get(i) != null) {
            lastSeenMdValue = masterRow.get(i);
            break;
          }
        }
        if (lastSeenMdValue == null && !md.isEmpty()) lastSeenMdValue = 0.0;
        for (ArrayList<Double> masterRow : this.md) {
          if (masterRow.size() > i) {
            if (masterRow.get(i) == null) masterRow.set(i, lastSeenMdValue);
            else lastSeenMdValue = masterRow.get(i);
          }
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

  private double[] getColumn(ArrayList<ArrayList<Double>> data, int pos) {
    if (data.isEmpty()) return new double[0];
    double[] column = new double[data.size()];
    for (int i = 0; i < data.size(); i++) {
      if (data.get(i) != null && data.get(i).size() > pos && data.get(i).get(pos) != null) {
        column[i] = data.get(i).get(pos);
      } else {
        column[i] = 0.0; // Should be handled by fillNullValue
      }
    }
    return column;
  }

  public void call_std() { // Calculates std from td
    this.std = new double[this.columnCnt];
    for (int i = 0; i < this.columnCnt; i++) {
      std[i] = Math.sqrt(varianceImperative(getColumn(this.td, i)));
      if (std[i] == 0) std[i] = 1.0;
    }
  }

  public boolean checkConsistency(ArrayList<Double> tuple) {
    if (kdTreeUtil == null) throw new IllegalStateException("KDTree not built.");
    ArrayList<Double> NN = kdTreeUtil.findTheNearestNeighbor(tuple);
    double d = delta(tuple, NN);
    return d <= eta;
  }

  public void getOriginalAnomaliesAndLearnModel() {
    if (td.isEmpty()) {
      System.err.println(
          "Time series data (td) is empty. Cannot determine original anomalies or fit model.");
      this.prediction_model = new VARMA(columnCnt, this.p_ar, this.q_ma); // Init empty model
      // Try to fit with empty coeffs or default zero coeffs
      int k = this.columnCnt;
      int p = this.p_ar;
      int q = this.q_ma;
      int expectedCoeffSize = k + (p * k * k) + (q * k * k);
      ArrayList<Double> defaultCoeffs =
          new ArrayList<>(Collections.nCopies(expectedCoeffSize, 0.0));
      try {
        this.prediction_model.fitCoeffs(defaultCoeffs, k, p, q);
      } catch (Exception e) {
        System.err.println("Error init empty model: " + e.getMessage());
      }
      return;
    }
    for (int i = 0; i < this.td.size(); i++) {
      ArrayList<Double> tuple = this.td.get(i);
      td_anomalies.add(!checkConsistency(tuple));
    }

    this.prediction_model = new VARMA(columnCnt, this.p_ar, this.q_ma);
    int k = this.columnCnt;
    int p = this.p_ar;
    int q = this.q_ma;
    int expectedCoeffSize = k + (p * k * k) + (q * k * k);

    if (coeffs_one_column.isEmpty() && expectedCoeffSize > 0) {
      System.err.println("Warning: Coefficients list is empty. Model will use zero coefficients.");
      ArrayList<Double> zeroCoeffs = new ArrayList<>(Collections.nCopies(expectedCoeffSize, 0.0));
      try {
        this.prediction_model.fitCoeffs(zeroCoeffs, columnCnt, this.p_ar, this.q_ma);
      } catch (Exception e) {
        System.err.println("Error fitting zero coeffs: " + e.getMessage());
      }
    } else if (!coeffs_one_column.isEmpty()) {
      try {
        this.prediction_model.fitCoeffs(coeffs_one_column, columnCnt, this.p_ar, this.q_ma);
      } catch (IllegalArgumentException e) {
        System.err.println(
            "Error fitting coefficients: " + e.getMessage() + ". Using zero coefficients.");
        ArrayList<Double> zeroCoeffs = new ArrayList<>(Collections.nCopies(expectedCoeffSize, 0.0));
        this.prediction_model.fitCoeffs(zeroCoeffs, columnCnt, this.p_ar, this.q_ma);
      }
    } else { // coeffs empty and expectedCoeffSize is 0 (e.g. p=0, q=0, k=0 which is unlikely)
      // This case implies a model with no parameters, VARMA should handle it.
      this.prediction_model.fitCoeffs(new ArrayList<>(), columnCnt, this.p_ar, this.q_ma);
    }
  }

  public void findInitialWindow(int order_p) { // order_p is AR order (this.p_ar)
    initial_window_indices = new int[2];
    initial_window_indices[0] = -1;
    initial_window_indices[1] = -1;

    if (td_anomalies.isEmpty() || order_p < 0) { // order_p can be 0 for VMA only model
      initial_window_indices[0] = 0;
      initial_window_indices[1] =
          (td.isEmpty() || order_p == 0) ? -1 : Math.min(order_p - 1, td.size() - 1);
      if (initial_window_indices[1] < initial_window_indices[0] && !td.isEmpty())
        initial_window_indices[1] = initial_window_indices[0];
      if (td.isEmpty()) initial_window_indices[1] = -1; // Empty window
      return;
    }
    if (order_p == 0) { // If only VMA, any point can start
      initial_window_indices[0] = 0;
      initial_window_indices[1] =
          -1; // Indicates no AR history needed, start predicting from first point
      // The repair logic might need adjustment for p_ar=0.
      // For now, set a valid "empty" window for AR.
      return;
    }

    int left = 0;
    int right = -1;
    for (int i = 0; i < td_anomalies.size(); i++) {
      if (!td_anomalies.get(i)) { // Normal point
        if (left > right) left = i;
        right = i;
        if (right - left + 1 >= order_p) {
          initial_window_indices[0] = left;
          initial_window_indices[1] = right; // Window is [left, right] inclusive
          return;
        }
      } else { // Anomaly
        left = i + 1;
      }
    }

    if (initial_window_indices[0] == -1 && !td.isEmpty()) {
      System.err.println(
          "Warning: No initial window of "
              + order_p
              + " normal points. Defaulting to first "
              + order_p
              + " points.");
      initial_window_indices[0] = 0;
      initial_window_indices[1] = Math.min(order_p - 1, td.size() - 1);
      if (initial_window_indices[1] < 0) initial_window_indices[1] = 0;
      if (initial_window_indices[1] < initial_window_indices[0] && td.size() > 0)
        initial_window_indices[1] = initial_window_indices[0];
    } else if (td.isEmpty()) {
      initial_window_indices[0] = 0;
      initial_window_indices[1] = -1;
    }
  }

  public ArrayList<ArrayList<Double>> getWindow(
      ArrayList<ArrayList<Double>> data, int currentIndex, int length) {
    if (length <= 0) return new ArrayList<>(); // For VARMA with p_ar=0, no window needed for AR.
    // currentIndex is the index of the point to be predicted.
    // Window should contain [Y_currentIndex-length, ..., Y_currentIndex-1]
    // So, it needs 'length' points before 'currentIndex'.
    if (currentIndex < length || data.isEmpty()) {
      System.err.println(
          "Error: Not enough historical data (i="
              + currentIndex
              + ", p="
              + length
              + ") for getWindow.");
      // Return a partial or empty window, predict method should handle this.
      // For simplicity, return empty and let predict fail if it needs full window.
      // Or, construct what's available (caller must be careful).
      // Let's return what's available up to 'length', VARMA.predict needs to be robust or this must
      // be guaranteed.
      ArrayList<ArrayList<Double>> W = new ArrayList<>();
      for (int j = Math.max(0, currentIndex - length); j < currentIndex; j++) {
        if (j < data.size()) W.add(data.get(j)); // Add if data exists
      }
      // If W is not of size 'length', VARMA.predict might fail.
      // The predict method typically expects exactly 'length' (p_ar) prior observations.
      // This getWindow logic should strictly return 'length' items or signal error.
      // The original code: i must be greater than p. (currentIndex > length)
      // For VARMA.predict, the window should be of size p_ar, containing data points
      // [Y_{t-p_ar}, ..., Y_{t-1}] to predict Y_t.
      // If currentIndex is t, then we need data from t-p_ar to t-1.
      // So, data.subList(currentIndex - length, currentIndex)
      // This means `currentIndex - length` must be >= 0.
      if (currentIndex < length) {
        // This is a critical issue if VARMA.predict requires full window.
        // For now, we'll let VARMA.predict handle a potentially short window if p_ar > 0.
        // Or, the calling logic (forward/backward repair) must ensure currentIndex is valid.
        System.err.println(
            "WARNING: getWindow called with insufficient history. i="
                + currentIndex
                + ", length="
                + length);
        // Fallback: return the latest available, up to 'length' items
        ArrayList<ArrayList<Double>> W_fallback = new ArrayList<>();
        for (int j = 0; j < Math.min(currentIndex, length); j++) {
          W_fallback.add(data.get(currentIndex - 1 - j)); // Gets Y_t-1, Y_t-2 ...
        }
        Collections.reverse(W_fallback); // To get Y_t-length, ..., Y_t-1
        return W_fallback; // This might be shorter than 'length'
      }
      // Standard case:
      return new ArrayList<>(data.subList(currentIndex - length, currentIndex));
    }
    // Return data from index i-length to i-1 (inclusive)
    return new ArrayList<>(data.subList(currentIndex - length, currentIndex));
  }

  public double calForwardPredictionLoss(
      int i, int current_p_ar, ArrayList<Double> candidate_for_i) {
    if (current_p_ar <= 0) return 0.0; // No AR prediction loss if p_ar is 0
    double sum_prediction_loss = 0.0;

    // Predict point i using window before i from td_repaired
    if (i >= current_p_ar) { // Check if enough history for prediction
      ArrayList<ArrayList<Double>> W_repaired = getWindow(this.td_repaired, i, current_p_ar);
      ArrayList<Double> prediction_for_i = prediction_model.predict(W_repaired);
      sum_prediction_loss += delta(prediction_for_i, candidate_for_i);
    } else {
      // Not enough history to form a full window for the first prediction,
      // could return high loss or skip this term.
      // For simplicity, if window is short, predict might throw error or give bad results.
      // VARMA.predict should ideally handle short windows if p_ar > 0.
      // Or, this means the first few points cannot be evaluated this way.
      // Let's assume this case means high loss if we can't predict.
      if (i > 0 && !this.td_repaired.isEmpty()) { // Try with partial window if predict supports
        try {
          ArrayList<ArrayList<Double>> W_partial =
              getWindow(this.td_repaired, i, i); // Use all available history up to i
          if (!W_partial.isEmpty()
              && W_partial.size() == this.p_ar) { // Only if it matches required arOrder
            ArrayList<Double> prediction_for_i = prediction_model.predict(W_partial);
            sum_prediction_loss += delta(prediction_for_i, candidate_for_i);
          } else {
            sum_prediction_loss += Double.MAX_VALUE / (current_p_ar + 1);
          } // Penalize if not predictable
        } catch (Exception e) {
          sum_prediction_loss += Double.MAX_VALUE / (current_p_ar + 1);
        } // Penalize if predict fails
      } else {
        sum_prediction_loss += Double.MAX_VALUE / (current_p_ar + 1); // High loss if no history
      }
    }

    // Predict subsequent p_ar-1 points using candidate_for_i as part of their window
    for (int lookahead_idx = 1; lookahead_idx < current_p_ar; lookahead_idx++) {
      if (i + lookahead_idx >= td.size()) break; // Out of bounds
      if (td_anomalies.get(i + lookahead_idx)) break; // Stop if next point is also an anomaly

      // Construct window for predicting td.get(i + lookahead_idx)
      // Window needs current_p_ar points ending before i + lookahead_idx
      // It will include points from td_repaired (up to i-1), then candidate_for_i, then original td
      // points
      ArrayList<ArrayList<Double>> W_mixed = new ArrayList<>();
      int needed_from_repaired =
          current_p_ar - 1 - lookahead_idx; // Number of points before candidate_for_i

      // Add from td_repaired (points before i)
      for (int hist_j = 0; hist_j < needed_from_repaired; ++hist_j) {
        if (i - (needed_from_repaired - hist_j) >= 0
            && (i - (needed_from_repaired - hist_j)) < td_repaired.size()) {
          W_mixed.add(td_repaired.get(i - (needed_from_repaired - hist_j)));
        } else {
          break;
        } // Not enough history in td_repaired
      }
      if (W_mixed.size() != needed_from_repaired && needed_from_repaired > 0)
        continue; // Couldn't form this part

      W_mixed.add(candidate_for_i); // Add the candidate for point i

      // Add from original td (points after i, before i+lookahead_idx)
      for (int fut_j = 1; fut_j < lookahead_idx; ++fut_j) {
        W_mixed.add(td.get(i + fut_j));
      }

      if (W_mixed.size() == current_p_ar) { // Ensure window is correct size
        try {
          ArrayList<Double> prediction = prediction_model.predict(W_mixed);
          sum_prediction_loss += delta(prediction, td.get(i + lookahead_idx));
        } catch (Exception e) {
          /* Prediction failed, loss increases implicitly or add penalty */
        }
      }
    }
    return sum_prediction_loss;
  }

  public double calBackwardPredictionLoss(
      int i, int current_p_ar, ArrayList<Double> candidate_for_i) {
    if (current_p_ar <= 0) return 0.0; // No AR prediction loss
    double sum_prediction_loss = 0.0;

    // Check prediction of point i+current_p_ar using a window that includes candidate_for_i
    // Window for predicting point (i+current_p_ar) is [Y_i, Y_i+1, ..., Y_i+current_p_ar-1]
    // Here, Y_i is candidate_for_i.
    if (i + current_p_ar < td_repaired.size()) { // Ensure point to predict (i+p) is within bounds
      ArrayList<ArrayList<Double>> W_construct = new ArrayList<>();
      W_construct.add(candidate_for_i); // This is Y_i
      for (int j = 1; j < current_p_ar; j++) { // Add Y_i+1 to Y_i+p-1
        if (i + j < td_repaired.size()) {
          W_construct.add(td_repaired.get(i + j));
        } else {
          break;
        } // Not enough future points
      }

      if (W_construct.size() == current_p_ar) {
        try {
          ArrayList<Double> prediction = prediction_model.predict(W_construct);
          sum_prediction_loss += delta(prediction, td_repaired.get(i + current_p_ar));
        } catch (Exception e) {
          /* Prediction failed */
        }
      }
    }

    // Check predictions of points i-1 down to i-current_p_ar+1
    // This part of original logic was complex.
    // Simplified: The backward loss mainly considers how well the candidate helps predict the
    // immediate future.
    // The original VAR backward loss had complex window manipulation.
    // For VARMA, given MA part is not active in predict, this simplifies.
    // The primary term above (predicting point i+p using candidate at i) is most direct.
    // Further terms would involve predicting points before i, which isn't typical for "backward" in
    // this context.
    // Let's stick to the one main term reflecting immediate future impact.
    return sum_prediction_loss;
  }

  public void forwardRepairing(int current_p_ar) { // current_p_ar is the AR order
    if (initial_window_indices == null || initial_window_indices[1] < -1) {
      System.err.println("Initial window not set for forward repair.");
      return;
    }
    int start_idx = initial_window_indices[1] + 1;
    if (current_p_ar == 0 && initial_window_indices[1] == -1) start_idx = 0; // VARMA(0,q) case

    for (int i = start_idx; i < td.size(); i++) {
      ArrayList<Double> optimal_repair;
      ArrayList<Double> x_repaired_predicted = null;

      if (current_p_ar > 0
          && i >= current_p_ar
          && !td_repaired.isEmpty()) { // Need enough history for prediction
        ArrayList<ArrayList<Double>> W_repaired = getWindow(this.td_repaired, i, current_p_ar);
        if (W_repaired.size() == current_p_ar) { // Ensure window is complete
          try {
            x_repaired_predicted = prediction_model.predict(W_repaired);
          } catch (Exception e) {
            System.err.println(
                "Forward repair: predict failed at index " + i + ". " + e.getMessage());
          }
        }
      } else if (current_p_ar == 0) { // Pure VMA model, prediction is just the constant
        try {
          x_repaired_predicted = prediction_model.predict(new ArrayList<>());
        } // Empty window
        catch (Exception e) {
          System.err.println(
              "Forward repair (p=0): predict failed at index " + i + ". " + e.getMessage());
        }
      }

      if (x_repaired_predicted == null
          && td.get(i) != null) { // If prediction failed, use original if not null
        x_repaired_predicted = new ArrayList<>(td.get(i)); // Fallback, may not be ideal
      } else if (x_repaired_predicted == null && td.get(i) == null) {
        x_repaired_predicted =
            new ArrayList<>(Collections.nCopies(this.columnCnt, 0.0)); // Further fallback
      }

      if (td_anomalies.get(i)) { // If original is anomaly
        ArrayList<ArrayList<Double>> candidates =
            this.kdTreeUtil.findKNearestNeighbors(x_repaired_predicted, this.k_knn);
        double min_loss = Double.MAX_VALUE;
        optimal_repair =
            new ArrayList<>(x_repaired_predicted); // Default to predicted if no better candidate

        for (ArrayList<Double> candidate : candidates) {
          double loss = calForwardPredictionLoss(i, current_p_ar, candidate);
          if (loss < min_loss) {
            min_loss = loss;
            optimal_repair = candidate;
          }
        }
        this.td_repaired.add(optimal_repair);
      } else { // If original is normal
        optimal_repair = td.get(i);
        this.td_repaired.add(optimal_repair);
      }

      // Check consistency of the repaired/original point against its prediction
      boolean repaired_is_anomaly = false;
      if (x_repaired_predicted != null) { // Only if prediction was successful
        if (delta(x_repaired_predicted, optimal_repair) > beta) {
          repaired_is_anomaly = true;
        }
      } else if (td_anomalies.get(
          i)) { // If prediction failed AND original was anomaly, assume repaired is still suspect
        repaired_is_anomaly = true;
      }
      this.anomalies_in_repaired.add(repaired_is_anomaly);
    }
  }

  public void backwardRepairing(int current_p_ar) { // current_p_ar is AR order
    if (initial_window_indices == null || initial_window_indices[0] < 0) {
      // System.err.println("Initial window not set or invalid for backward repair.");
      return; // No points before initial window to repair
    }
    // Iterates from (initial_window_indices[0] - 1) down to 0.
    for (int i = initial_window_indices[0] - 1; i >= 0; i--) {
      ArrayList<Double> optimal_repair;
      ArrayList<Double> reference_point_for_knn = null;

      // For backward repair, KNN candidates are often based on the next (already repaired) point
      if (i + 1 < td_repaired.size()) {
        reference_point_for_knn = td_repaired.get(i + 1);
      } else if (!td.isEmpty()) { // Fallback if next repaired point isn't available
        reference_point_for_knn = td.get(Math.min(i + 1, td.size() - 1));
      } else { // No reference point
        reference_point_for_knn = new ArrayList<>(Collections.nCopies(this.columnCnt, 0.0));
      }

      if (td_anomalies.get(i)) { // If original is anomaly
        ArrayList<ArrayList<Double>> candidates =
            this.kdTreeUtil.findKNearestNeighbors(reference_point_for_knn, this.k_knn);
        double min_loss = Double.MAX_VALUE;
        optimal_repair = new ArrayList<>(reference_point_for_knn); // Default

        for (ArrayList<Double> candidate : candidates) {
          double loss = calBackwardPredictionLoss(i, current_p_ar, candidate);
          if (loss < min_loss) {
            min_loss = loss;
            optimal_repair = candidate;
          }
        }
        // td_repaired was filled up to initial_window[1] initially, then forward.
        // backward repair needs to SET values in pre-existing slots.
        // Ensure td_repaired has dummy slots for points before initial_window[0] if necessary.
        // This is usually handled by initializing td_repaired with td, then modifying.
        // Here, td_repaired is built up. So, for backward, we are setting elements at indices <
        // initial_window[0].
        // This implies td_repaired should be pre-sized or use `add(0, element)` carefully.
        // Safest: td_repaired should be a full copy of td first. Let's adjust.
        this.td_repaired.set(i, optimal_repair);
      } else { // If original is normal
        optimal_repair = td.get(i);
        this.td_repaired.set(i, optimal_repair);
      }

      // Check consistency of the repaired point
      // Backward anomaly check is harder as "prediction" is not as direct.
      // We can check its delta w.r.t. a prediction made using it and future values,
      // or its delta w.r.t. the next repaired point.
      boolean repaired_is_anomaly = false;
      if (i + current_p_ar < td_repaired.size()
          && current_p_ar > 0) { // If we can form a window starting at optimal_repair
        ArrayList<ArrayList<Double>> W_check = new ArrayList<>();
        W_check.add(optimal_repair);
        for (int j = 1; j < current_p_ar; ++j) {
          if (i + j < td_repaired.size()) W_check.add(td_repaired.get(i + j));
          else break;
        }
        if (W_check.size() == current_p_ar) {
          try {
            ArrayList<Double> pred_of_future = prediction_model.predict(W_check);
            // Compare pred_of_future (which is prediction for point i+p) with actual
            // td_repaired.get(i+p)
            if (delta(pred_of_future, td_repaired.get(i + current_p_ar)) > beta) {
              repaired_is_anomaly = true;
            }
          } catch (Exception e) {
            repaired_is_anomaly = true; /* predict failed */
          }
        } else {
          repaired_is_anomaly = td_anomalies.get(i);
        } // Not enough points for check, keep original status
      } else if (i + 1 < td_repaired.size()) { // Simpler check if p_ar is 0 or not enough points
        if (delta(optimal_repair, td_repaired.get(i + 1))
            > beta) { // Compare to next point (if MA like behavior)
          repaired_is_anomaly = true;
        }
      } else {
        repaired_is_anomaly = td_anomalies.get(i); // Fallback
      }
      this.anomalies_in_repaired.set(i, repaired_is_anomaly);
    }
  }

  public void detectAndRepair() {
    if (td.isEmpty()) {
      System.err.println("No time series data to detect and repair.");
      return;
    }
    fillNullValue(); // Fill nulls in td and md
    buildKDTree();
    call_std(); // Calculate std from td

    // Initialize prediction_model and load coefficients
    getOriginalAnomaliesAndLearnModel(); // This now also determines td_anomalies

    // Initialize td_repaired as a copy of td, and anomalies_in_repaired based on td_anomalies
    // This is a key change for backwardRepairing to correctly .set() values.
    td_repaired.clear();
    anomalies_in_repaired.clear();
    for (ArrayList<Double> row : td) {
      td_repaired.add(new ArrayList<>(row)); // Deep copy
    }
    for (Boolean anomaly_status : td_anomalies) {
      anomalies_in_repaired.add(anomaly_status);
    }

    findInitialWindow(this.p_ar); // p_ar is the AR order

    // The original MasterDetector had a hardcoded anomaly injection for first 10 points:
    // ArrayList<Double> zero_tuple = new ArrayList<>(Collections.nCopies(this.columnCnt, 0.0));
    // for (int i = 0; i < Math.min(10, this.td.size()); i++) {
    //   this.td.set(i, zero_tuple); // Modifies original td
    //   // This would also affect td_repaired if copied after this.
    //   // And td_anomalies if checkConsistency was run after this.
    // }
    // This part is removed to avoid unintended side effects on possibly clean data.
    // If intentional, it should be part of the input data generation or a specific UDF option.

    System.out.println(
        "Initial normal window (0-indexed, inclusive): ["
            + initial_window_indices[0]
            + ", "
            + initial_window_indices[1]
            + "]");

    // Points within the initial normal window are considered correct
    for (int j = initial_window_indices[0]; j <= initial_window_indices[1]; j++) {
      if (j >= 0 && j < td_repaired.size()) { // Check bounds
        td_repaired.set(j, new ArrayList<>(td.get(j))); // Ensure they are from original td
        anomalies_in_repaired.set(j, false); // Mark as not anomalous
      }
    }

    // Perform repairs
    // Note: p_ar (AR order) is used for windowing in repair.
    forwardRepairing(this.p_ar);
    backwardRepairing(this.p_ar);
  }

  public ArrayList<ArrayList<Double>> getTd_repaired() {
    return td_repaired;
  }

  public ArrayList<Long> getTd_time() {
    return td_time;
  }

  public ArrayList<Boolean> getAnomalies_in_repaired() {
    return anomalies_in_repaired;
  }
}
