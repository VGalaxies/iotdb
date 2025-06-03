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

import java.util.ArrayList;
import java.util.Collections;

public class VARMA {
  private final int numSeries; // k: Number of time series
  private final int arOrder; // p: AR order
  private final int maOrder; // q: MA order

  // Coefficients
  private Matrix constantVectorC; // k x 1 vector of constants
  private ArrayList<Matrix> coeffsAr; // List of p matrices, each k x k (Phi_1, ..., Phi_p)
  private ArrayList<Matrix> coeffsMa; // List of q matrices, each k x k (Theta_1, ..., Theta_q)

  public VARMA(int numSeries, int arOrder, int maOrder) {
    this.numSeries = numSeries;
    this.arOrder = arOrder;
    this.maOrder = maOrder;

    // Initialize structures
    this.constantVectorC = new Matrix(numSeries, 1); // Filled with zeros initially
    this.coeffsAr = new ArrayList<>(arOrder);
    for (int i = 0; i < arOrder; i++) {
      this.coeffsAr.add(new Matrix(numSeries, numSeries)); // k x k zero matrix
    }
    this.coeffsMa = new ArrayList<>(maOrder);
    for (int i = 0; i < maOrder; i++) {
      this.coeffsMa.add(new Matrix(numSeries, numSeries)); // k x k zero matrix
    }
  }

  // Train the AR part of the model with data to get coefficients. MA coeffs set to zero.
  public void fit(ArrayList<ArrayList<Double>> data) {
    int nObs = data.size(); // Number of observations (rows)
    int k = this.numSeries; // Number of series (columns)
    int p = this.arOrder; // AR order

    if (p <= 0) {
      // If no AR part, only constants (could be means, or zero if not estimated)
      // For simplicity, if p=0, set constants to zero and AR coeffs are empty.
      // A more sophisticated approach would estimate means for C if p=0.
      // Here, constants are part of the VAR regression. If p=0, this fit method isn't suitable.
      System.err.println(
          "AR order p must be > 0 for this VARMA fit method. Setting trivial model.");
      this.constantVectorC = new Matrix(k, 1); // Zero constants
      this.coeffsAr.clear(); // No AR coefficient matrices
      // MA coefficients remain zero matrices as initialized
      for (int i = 0; i < maOrder; i++) {
        this.coeffsMa.set(i, new Matrix(k, k));
      }
      return;
    }

    if (nObs <= p) {
      throw new IllegalArgumentException(
          "Not enough data points (got " + nObs + ") to fit VARMA model of AR order p=" + p);
    }

    // Construct the design matrix X_design and target matrix Y_target
    // Each row of X_design: [1, Y_1,t-1, ..., Y_k,t-1,  ...,  Y_1,t-p, ..., Y_k,t-p]
    // Dimension of each X_design row: 1 (for constant) + p * k
    int xRowDim = 1 + p * k;
    ArrayList<ArrayList<Double>> X_design_list = new ArrayList<>();
    ArrayList<ArrayList<Double>> Y_target_list = new ArrayList<>();

    for (int t = p; t < nObs; t++) { // Start from observation p up to nObs-1
      // Construct Y_target row (current observation)
      Y_target_list.add(data.get(t));

      // Construct X_design row
      ArrayList<Double> x_row = new ArrayList<>();
      x_row.add(1.0); // Constant term
      for (int lag = 0; lag < p; lag++) { // For each lag from 1 to p
        // data.get(t - (lag+1)) corresponds to Y_{t-(lag+1)}
        x_row.addAll(data.get(t - (lag + 1)));
      }
      X_design_list.add(x_row);
    }

    Matrix Xmat = new Matrix(X_design_list);
    Matrix Ymat = new Matrix(Y_target_list); // This is (nObs-p) x k

    try {
      Matrix XtX = Xmat.transpose().multiply(Xmat);
      Matrix XtY = Xmat.transpose().multiply(Ymat); // Ymat needs to be (nObs-p) x k
      // XtY becomes (1+pk) x k
      Matrix betaHat = XtX.solve(XtY); // betaHat is (1+pk) x k

      // Extract constant vector C (first row of betaHat, transposed)
      this.constantVectorC = new Matrix(k, 1);
      for (int i = 0; i < k; ++i) {
        this.constantVectorC.data.get(i).set(0, betaHat.data.get(0).get(i));
      }

      // Extract AR coefficient matrices Phi_s
      this.coeffsAr.clear();
      for (int s = 0; s < p; s++) { // For Phi_1 to Phi_p
        Matrix phi_s = new Matrix(k, k);
        for (int row_k = 0; row_k < k; row_k++) { // k_th equation (for Y_k,t)
          for (int col_k = 0; col_k < k; col_k++) { // Coefficient for Y_col_k, t-(s+1)
            // betaHat row index: 1 (for const) + s*k (prev lags) + col_k (current lag series)
            // betaHat col index: row_k (for which Y target)
            phi_s.data.get(row_k).set(col_k, betaHat.data.get(1 + s * k + col_k).get(row_k));
          }
        }
        this.coeffsAr.add(phi_s);
      }
    } catch (IllegalArgumentException e) {
      System.err.println(
          "Error fitting VARMA AR coefficients: " + e.getMessage() + ". Setting trivial model.");
      this.constantVectorC = new Matrix(k, 1); // Zero constants
      this.coeffsAr.clear();
      for (int i = 0; i < arOrder; i++) this.coeffsAr.add(new Matrix(k, k)); // Zero AR coeffs
    }

    // 如果指定了 MA 阶数 > 0，则先用 AR 部分拟合结果计算残差，再拟合 MA 部分
    this.coeffsMa.clear();
    if (maOrder <= 0) {
      // q=0 时，直接保留零矩阵
      for (int i = 0; i < maOrder; i++) {
        this.coeffsMa.add(new Matrix(k, k));
      }
    } else {
      // 1) 先用 AR(部分)计算残差 residuals
      int sz = data.size();
      ArrayList<ArrayList<Double>> Y_ar = new ArrayList<>();
      ArrayList<ArrayList<Double>> X_ar = new ArrayList<>();
      for (int t = p; t < sz; t++) {
        // 当前观测 Y_t
        Y_ar.add(data.get(t));
        // 构造 X_row = [1, Y_{t-1}, Y_{t-2}, ..., Y_{t-p}]
        ArrayList<Double> xRow = new ArrayList<>();
        xRow.add(1.0);
        for (int lag = 0; lag < p; lag++) {
          xRow.addAll(data.get(t - (lag + 1)));
        }
        X_ar.add(xRow);
      }
      Matrix Xmat_ar = new Matrix(X_ar);
      Matrix Ymat_ar = new Matrix(Y_ar);
      // 用已拟合的常数和 AR 系数，对每个时刻做一次前向预测，计算残差
      ArrayList<ArrayList<Double>> residuals = new ArrayList<>();
      for (int i = 0; i < Y_ar.size(); i++) {
        // 计算 yHat = C + sum_{s=1..p} Phi_s * Y_{t-s}
        Matrix yHat = new Matrix(this.constantVectorC.data);
        for (int s = 0; s < p; s++) {
          Matrix phi_s = this.coeffsAr.get(s);
          ArrayList<Double> lagData = data.get(p + i - (s + 1));
          Matrix lagVec = new Matrix(k, 1);
          for (int idx = 0; idx < k; idx++) {
            lagVec.data.get(idx).set(0, lagData.get(idx));
          }
          yHat = yHat.add(phi_s.multiply(lagVec));
        }
        // 计算残差 E_t = Y_t - yHat
        ArrayList<Double> resRow = new ArrayList<>();
        for (int idx = 0; idx < k; idx++) {
          double e = Y_ar.get(i).get(idx) - yHat.data.get(idx).get(0);
          resRow.add(e);
        }
        residuals.add(resRow);
      }

      // 2) 组装 MA 设计矩阵 X_ma 和目标矩阵 Y_ma
      int m = residuals.size();
      ArrayList<ArrayList<Double>> X_ma = new ArrayList<>();
      ArrayList<ArrayList<Double>> Y_ma = new ArrayList<>();
      for (int t = maOrder; t < m; t++) {
        // 当前目标 E_t
        Y_ma.add(residuals.get(t));
        // 设计行 = [ E_{t-1}, E_{t-2}, ..., E_{t-q} ]
        ArrayList<Double> xRow = new ArrayList<>();
        for (int lag = 1; lag <= maOrder; lag++) {
          xRow.addAll(residuals.get(t - lag));
        }
        X_ma.add(xRow);
      }

      // 3) 用最小二乘求解 MA 系数矩阵
      if (!X_ma.isEmpty()) {
        Matrix Xmat_ma = new Matrix(X_ma);
        Matrix Ymat_ma = new Matrix(Y_ma);
        Matrix XtX_ma = Xmat_ma.transpose().multiply(Xmat_ma);
        Matrix XtY_ma = Xmat_ma.transpose().multiply(Ymat_ma);
        Matrix betaHat_ma = XtX_ma.solve(XtY_ma);

        // betaHat_ma 大小 = (q*k) x k，将其拆成 q 个 k×k 矩阵
        for (int s = 0; s < maOrder; s++) {
          Matrix theta_s = new Matrix(k, k);
          for (int i_row = 0; i_row < k; i_row++) {
            for (int j_col = 0; j_col < k; j_col++) {
              // 在 betaHat_ma 中，行索引 = s*k + j_col, 列索引 = i_row
              theta_s.data.get(i_row).set(j_col, betaHat_ma.data.get(s * k + j_col).get(i_row));
            }
          }
          this.coeffsMa.add(theta_s);
        }
      } else {
        // 样本不足，补 q 个零矩阵
        for (int i = 0; i < maOrder; i++) {
          this.coeffsMa.add(new Matrix(k, k));
        }
      }
    }
  }

  // Load coefficients from a flattened list
  public void fitCoeffs(
      ArrayList<Double> coeffsInOneColumn, int numSeries, int arOrder, int maOrder) {
    if (numSeries != this.numSeries || arOrder != this.arOrder || maOrder != this.maOrder) {
      throw new IllegalArgumentException("Mismatch in VARMA parameters during fitCoeffs.");
    }

    int k = this.numSeries;
    int p = this.arOrder;
    int q = this.maOrder;
    int expectedSize = k + (p * k * k) + (q * k * k); // C (k) + Phi's (p*k^2) + Theta's (q*k^2)

    if (coeffsInOneColumn.size() != expectedSize) {
      throw new IllegalArgumentException(
          "coeffsInOneColumn has incorrect size. Expected: "
              + expectedSize
              + ", Got: "
              + coeffsInOneColumn.size());
    }

    int currentIndex = 0;

    // Constant vector C (k x 1)
    this.constantVectorC = new Matrix(k, 1);
    for (int i = 0; i < k; i++) {
      this.constantVectorC.data.get(i).set(0, coeffsInOneColumn.get(currentIndex++));
    }

    // AR coefficient matrices (Phi_s are k x k)
    this.coeffsAr.clear();
    for (int s = 0; s < p; s++) {
      Matrix phi_s = new Matrix(k, k);
      for (int i = 0; i < k; i++) { // row
        for (int j = 0; j < k; j++) { // col
          phi_s.data.get(i).set(j, coeffsInOneColumn.get(currentIndex++));
        }
      }
      this.coeffsAr.add(phi_s);
    }

    // MA coefficient matrices (Theta_s are k x k)
    this.coeffsMa.clear();
    for (int s = 0; s < q; s++) {
      Matrix theta_s = new Matrix(k, k);
      for (int i = 0; i < k; i++) { // row
        for (int j = 0; j < k; j++) { // col
          theta_s.data.get(i).set(j, coeffsInOneColumn.get(currentIndex++));
        }
      }
      this.coeffsMa.add(theta_s);
    }
  }

  // Predict one step ahead
  // Window should have 'arOrder' rows, each row being a k-variate observation (Y_t-1, ..., Y_t-p)
  public ArrayList<Double> predict(ArrayList<ArrayList<Double>> window) {
    int k = this.numSeries;
    int p = this.arOrder;

    if (p > 0 && (window == null || window.isEmpty() || window.size() != p)) {
      throw new IllegalArgumentException(
          "Window size must be equal to AR order ("
              + p
              + ") for prediction. Got: "
              + (window == null ? 0 : window.size()));
    }
    if (p > 0 && window.get(0).size() != k) {
      throw new IllegalArgumentException(
          "Window column count must be equal to number of series (" + k + ").");
    }

    // Initialize prediction with constant vector C
    Matrix yHatMatrix = new Matrix(this.constantVectorC.data); // Creates a copy

    // AR part: Sum(Phi_s * Y_t-s)
    if (p > 0) {
      for (int s = 0; s < p; s++) {
        // window.get(s) contains Y_t-(s+1) if window is [Y_t-1, Y_t-2, ..., Y_t-p]
        // The VARMA formula uses Y_t-1, Y_t-2,...
        // If window is [Y_t-p, ..., Y_t-1] (chronological order of past lags), then
        // Y_t-(s+1) is window.get(p - 1 - s)
        Matrix lagVectorY = new Matrix(k, 1);
        ArrayList<Double> lagData = window.get(p - 1 - s); // Y_{t-(s+1)}
        for (int i = 0; i < k; i++) {
          lagVectorY.data.get(i).set(0, lagData.get(i));
        }
        Matrix phi_s = this.coeffsAr.get(s); // Phi_{s+1}
        Matrix term = phi_s.multiply(lagVectorY);
        yHatMatrix = yHatMatrix.add(term); // yHat = yHat + Phi_s * Y_t-s
      }
    }

    // MA part: Sum(Theta_s * E_t-s)
    // As discussed, true MA prediction requires past multivariate residuals (E_t-s).
    // For this simplified version, we assume E_t-s = 0 for prediction.
    // So, MA terms (Theta_s * E_t-s) effectively become 0.

    ArrayList<Double> predictionTuple = new ArrayList<>(k);
    for (int i = 0; i < k; i++) {
      predictionTuple.add(yHatMatrix.data.get(i).get(0));
    }
    return predictionTuple;
  }

  public ArrayList<Double> getCoeffsInOneColumn() {
    ArrayList<Double> coeffsOneColumn = new ArrayList<>();
    int k = this.numSeries;
    int p = this.arOrder;
    int q = this.maOrder;

    // Add constant vector C (k elements)
    for (int i = 0; i < k; i++) {
      coeffsOneColumn.add(this.constantVectorC.data.get(i).get(0));
    }

    // Add AR coefficient matrices (Phi_s, each k*k elements, row by row)
    for (int s = 0; s < p; s++) {
      Matrix phi_s = this.coeffsAr.get(s);
      for (int i = 0; i < k; i++) {
        for (int j = 0; j < k; j++) {
          coeffsOneColumn.add(phi_s.data.get(i).get(j));
        }
      }
    }

    // Add MA coefficient matrices (Theta_s, each k*k elements, row by row)
    for (int s = 0; s < q; s++) {
      Matrix theta_s = this.coeffsMa.get(s);
      for (int i = 0; i < k; i++) {
        for (int j = 0; j < k; j++) {
          coeffsOneColumn.add(theta_s.data.get(i).get(j));
        }
      }
    }
    return coeffsOneColumn;
  }

  // Helper class for matrix operations (adapted from original VAR.java/VARMA.java)
  public static class Matrix {
    public final int m; // rows
    public final int n; // cols
    public final ArrayList<ArrayList<Double>> data;

    public Matrix(int m, int n) {
      this.m = m;
      this.n = n;
      this.data = new ArrayList<>(m);
      for (int i = 0; i < m; i++) {
        ArrayList<Double> row = new ArrayList<>(Collections.nCopies(n, 0.0));
        this.data.add(row);
      }
    }

    public Matrix(ArrayList<ArrayList<Double>> data) {
      if (data == null || data.isEmpty()) {
        throw new IllegalArgumentException("Input data for Matrix cannot be null or empty.");
      }
      this.m = data.size();
      this.n = data.get(0).size();
      this.data = new ArrayList<>(m);
      for (int i = 0; i < m; i++) {
        if (data.get(i).size() != this.n) {
          throw new IllegalArgumentException(
              "All rows in Matrix data must have the same number of columns.");
        }
        this.data.add(new ArrayList<>(data.get(i))); // Deep copy row
      }
    }

    public Matrix transpose() {
      Matrix C = new Matrix(n, m);
      for (int i = 0; i < m; i++) {
        for (int j = 0; j < n; j++) {
          C.data.get(j).set(i, this.data.get(i).get(j));
        }
      }
      return C;
    }

    public Matrix multiply(Matrix B) {
      if (this.n != B.m) {
        throw new IllegalArgumentException(
            "Matrix dimensions don't match for multiplication: A.n ("
                + this.n
                + ") != B.m ("
                + B.m
                + ")");
      }
      Matrix C = new Matrix(this.m, B.n);
      for (int i = 0; i < C.m; i++) {
        for (int j = 0; j < C.n; j++) {
          double sum = 0.0;
          for (int l = 0; l < this.n; l++) {
            sum += this.data.get(i).get(l) * B.data.get(l).get(j);
          }
          C.data.get(i).set(j, sum);
        }
      }
      return C;
    }

    public Matrix add(Matrix B) {
      if (this.m != B.m || this.n != B.n) {
        throw new IllegalArgumentException("Matrix dimensions must agree for addition.");
      }
      Matrix C = new Matrix(this.m, this.n);
      for (int i = 0; i < this.m; i++) {
        for (int j = 0; j < this.n; j++) {
          C.data.get(i).set(j, this.data.get(i).get(j) + B.data.get(i).get(j));
        }
      }
      return C;
    }

    public Matrix solve(Matrix B) { // Solves AX = B, where A is this matrix (XtX)
      Matrix A = this;
      if (A.m != A.n || A.m != B.m) {
        throw new IllegalArgumentException(
            "Matrix dimensions don't match for solving linear system. A must be square and A.m == B.m.");
      }
      if (A.m == 0) {
        throw new IllegalArgumentException("Cannot solve for an empty matrix A.");
      }

      Matrix[] LU = A.luDecompositionWithPivoting();
      Matrix L = LU[0];
      Matrix U = LU[1];
      Matrix P = LU[2]; // Permutation matrix

      Matrix PB = P.multiply(B);

      // Solve LY = PB using forward substitution
      Matrix Y = new Matrix(A.n, B.n);
      for (int j_col = 0; j_col < B.n; j_col++) {
        for (int i_row = 0; i_row < A.n; i_row++) {
          double sum_val = 0.0;
          for (int k_col = 0; k_col < i_row; k_col++) {
            sum_val += L.data.get(i_row).get(k_col) * Y.data.get(k_col).get(j_col);
          }
          if (Math.abs(L.data.get(i_row).get(i_row)) < 1e-12) {
            // This check is on L's diagonal, which should be 1s for Doolittle.
            // If using Cholesky or other LU, ensure L is non-singular.
            // For general LU, U carries singularity info on its diagonal.
          }
          Y.data
              .get(i_row)
              .set(j_col, (PB.data.get(i_row).get(j_col) - sum_val) / L.data.get(i_row).get(i_row));
        }
      }

      // Solve UX = Y using backward substitution
      Matrix X = new Matrix(A.n, B.n);
      for (int j_col = 0; j_col < B.n; j_col++) {
        for (int i_row = A.n - 1; i_row >= 0; i_row--) {
          double sum_val = 0.0;
          for (int k_col = i_row + 1; k_col < A.n; k_col++) {
            sum_val += U.data.get(i_row).get(k_col) * X.data.get(k_col).get(j_col);
          }
          if (Math.abs(U.data.get(i_row).get(i_row)) < 1e-12) {
            throw new IllegalArgumentException(
                "Matrix is singular or nearly singular (U["
                    + i_row
                    + "]["
                    + i_row
                    + "] is near zero). Cannot solve.");
          }
          X.data
              .get(i_row)
              .set(j_col, (Y.data.get(i_row).get(j_col) - sum_val) / U.data.get(i_row).get(i_row));
        }
      }
      return X;
    }

    // LU decomposition with partial pivoting (Doolittle algorithm)
    // Returns L, U, P such that PA = LU
    public Matrix[] luDecompositionWithPivoting() {
      Matrix A = this;
      if (A.m != A.n) {
        throw new IllegalArgumentException("Matrix must be square for LU decomposition.");
      }
      int n_dim = A.n;
      Matrix L = new Matrix(n_dim, n_dim);
      Matrix U = new Matrix(A.data); // Start U as a copy of A
      Matrix P = Matrix.identity(n_dim); // Permutation matrix, initially identity

      for (int k = 0; k < n_dim; k++) {
        // Pivoting: find max element in current column k at or below diagonal
        int maxRow = k;
        double maxVal = Math.abs(U.data.get(k).get(k));
        for (int i = k + 1; i < n_dim; i++) {
          if (Math.abs(U.data.get(i).get(k)) > maxVal) {
            maxVal = Math.abs(U.data.get(i).get(k));
            maxRow = i;
          }
        }

        if (Math.abs(U.data.get(maxRow).get(k)) < 1e-12) { // Check for singularity
          // No non-zero pivot found or pivot is too small
          // throw new IllegalArgumentException("Matrix is singular or nearly singular during LU
          // decomposition (pivot near zero at column " + k + ").");
          // Continue, but solution might be unstable or indicate singularity later
        }

        // Swap rows in U and P
        if (maxRow != k) {
          Collections.swap(U.data, k, maxRow);
          Collections.swap(P.data, k, maxRow);
          // Also need to swap corresponding elements in L that are already computed
          // For Doolittle, L elements are computed based on U, so this is tricky.
          // Simpler LUP: swap in original A copy (which U is), then compute L elements.
          // For L elements already computed in columns < k, swap rows in L.
          for (int col_l = 0; col_l < k; col_l++) {
            double temp = L.data.get(k).get(col_l);
            L.data.get(k).set(col_l, L.data.get(maxRow).get(col_l));
            L.data.get(maxRow).set(col_l, temp);
          }
        }

        L.data.get(k).set(k, 1.0); // Diagonal of L is 1 for Doolittle

        // Compute U and L
        for (int j = k; j < n_dim; j++) { // For U row k
          // U.data.get(k).set(j, U.data.get(k).get(j)); // Already there from copy/swap
        }
        for (int i = k + 1; i < n_dim; i++) { // For L column k
          if (Math.abs(U.data.get(k).get(k)) < 1e-12) { // Check divisor
            if (Math.abs(U.data.get(i).get(k))
                > 1e-9) { // If element to eliminate is non-zero but pivot is zero
              throw new IllegalArgumentException(
                  "Matrix is singular (zero pivot with non-zero element below) at U["
                      + k
                      + "]["
                      + k
                      + "].");
            }
            L.data.get(i).set(k, 0.0); // Element U[i][k] must be zero if pivot U[k][k] is zero
          } else {
            L.data.get(i).set(k, U.data.get(i).get(k) / U.data.get(k).get(k));
          }
          for (int j = k; j < n_dim; j++) { // Update U row i
            U.data
                .get(i)
                .set(j, U.data.get(i).get(j) - L.data.get(i).get(k) * U.data.get(k).get(j));
          }
        }
      }
      return new Matrix[] {L, U, P};
    }

    public static Matrix identity(int size) {
      Matrix I = new Matrix(size, size);
      for (int i = 0; i < size; i++) {
        I.data.get(i).set(i, 1.0);
      }
      return I;
    }

    public ArrayList<ArrayList<Double>> getData() {
      return data;
    }
  }
}
