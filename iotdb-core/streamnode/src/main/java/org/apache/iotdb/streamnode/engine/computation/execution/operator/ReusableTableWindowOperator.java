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

package org.apache.iotdb.streamnode.engine.computation.execution.operator;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.execution.operator.process.window.TableWindowOperator;
import org.apache.iotdb.streamnode.engine.computation.execution.StreamOperatorContext;

public class ReusableTableWindowOperator extends TableWindowOperator {

  public ReusableTableWindowOperator(TableWindowOperator tableWindowOperator) {
    super(tableWindowOperator, tableWindowOperator.getInputOperator());
  }

  public ReusableTableWindowOperator(
      ReusableTableWindowOperator tableWindowOperator, Operator inputOperator) {
    super(tableWindowOperator, inputOperator);
  }

  @Override
  public void close() throws Exception {
    if (((StreamOperatorContext) getOperatorContext()).getDriverContext().isTaskClosing()) {
      super.close();
      return;
    }
    releaseReservedMemory();
    getInputOperator().close();
  }
}
