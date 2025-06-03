package org.apache.iotdb.library.anomaly;

// Changed
import org.apache.iotdb.library.anomaly.util.VARMADetector;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.util.ArrayList;
import java.util.Objects;

public class UDTFVARMADetect implements UDTF {

  private VARMADetector masterDetector;
  private int output_column_idx; // 0-indexed
  private String output_type;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    for (int i = 0; i < validator.getParameters().getAttributes().size(); i++) {
      validator.validateInputSeriesDataType(i, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64);
    }
    if (validator.getParameters().hasAttribute("k")) {
      validator.validate(
          k_val -> (int) k_val > 0,
          "Parameter k (for KNN) should be a positive integer.",
          validator.getParameters().getInt("k"));
    }
    // Validate 'p' (AR order)
    if (validator.getParameters().hasAttribute("p")) {
      validator.validate(
          p_val -> (int) p_val >= 0, // AR order can be 0 for VMA model
          "Parameter p (AR order) should be a non-negative integer.",
          validator.getParameters().getInt("p"));
    }
    // Validate 'q' (MA order) - new parameter for VARMA
    if (validator.getParameters().hasAttribute("q")) {
      validator.validate(
          q_val -> (int) q_val >= 0, // MA order can be 0 for VAR model
          "Parameter q (MA order) should be a non-negative integer.",
          validator.getParameters().getInt("q"));
    }
    if (validator.getParameters().hasAttribute("output_column")) {
      validator.validate(
          oc_val -> (int) oc_val >= 1, // 1-indexed
          "Parameter output_column should be a positive integer.",
          validator.getParameters().getInt("output_column"));
    }
    if (validator.getParameters().hasAttribute("eta")) {
      validator.validate(
          eta_val -> (double) eta_val > 0,
          "Parameter eta should be larger than 0.",
          validator.getParameters().getDouble("eta"));
    }
    if (validator.getParameters().hasAttribute("beta")) {
      validator.validate(
          beta_val -> (double) beta_val > 0,
          "Parameter beta should be larger than 0.",
          validator.getParameters().getDouble("beta"));
    }
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy());
    output_type = parameters.getStringOrDefault("output_type", "repair");

    if (output_type.equalsIgnoreCase("repair") || output_type.equalsIgnoreCase("repairing")) {
      configurations.setOutputDataType(Type.DOUBLE);
    } else { // "anomaly" or other defaults to boolean
      configurations.setOutputDataType(Type.BOOLEAN);
    }

    int numAllInputs = parameters.getDataTypes().size();
    if ((numAllInputs - 1) % 2 != 0 || numAllInputs < 3) {
      throw new IllegalArgumentException(
          "Input series count does not match expected structure (N ts_cols, N md_cols, 1 coeff_col). Need (2N+1) series.");
    }
    int columnCnt = (numAllInputs - 1) / 2;

    int k_knn = parameters.getIntOrDefault("k", 3);
    int p_ar_order = parameters.getIntOrDefault("p", 1); // Default AR order
    int q_ma_order = parameters.getIntOrDefault("q", 0); // Default MA order
    double eta = parameters.getDoubleOrDefault("eta", 1.0);
    double beta = parameters.getDoubleOrDefault("beta", 1.0);

    this.output_column_idx = parameters.getIntOrDefault("output_column", 1) - 1; // 0-indexed
    if (this.output_column_idx < 0 || this.output_column_idx >= columnCnt) {
      throw new IllegalArgumentException(
          "output_column index "
              + (this.output_column_idx + 1)
              + " is out of bounds for "
              + columnCnt
              + " time series columns.");
    }

    // Pass q_ma_order to MasterDetector
    masterDetector = new VARMADetector(columnCnt, k_knn, p_ar_order, q_ma_order, eta, beta);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (!masterDetector.isNullRow(row)) {
      masterDetector.addRow(row);
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    masterDetector.detectAndRepair();

    ArrayList<ArrayList<Double>> td_repaired = masterDetector.getTd_repaired();
    ArrayList<Long> td_time = masterDetector.getTd_time();
    ArrayList<Boolean> anomalies_in_repaired = masterDetector.getAnomalies_in_repaired();

    if (Objects.equals(output_type, "repair") || Objects.equals(output_type, "repairing")) {
      for (int i = 0; i < td_repaired.size(); i++) {
        if (i < td_time.size()
            && td_repaired.get(i) != null
            && output_column_idx < td_repaired.get(i).size()
            && td_repaired.get(i).get(output_column_idx) != null) {
          collector.putDouble(td_time.get(i), td_repaired.get(i).get(output_column_idx));
        }
      }
    } else { // "anomaly" or other types output boolean
      for (int i = 0; i < anomalies_in_repaired.size(); i++) {
        if (i < td_time.size()) { // Ensure time exists for this anomaly status
          collector.putBoolean(td_time.get(i), anomalies_in_repaired.get(i));
        }
      }
    }
  }
}
