/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.streamnode.engine.window;

import org.apache.iotdb.commons.stream.StreamWindow;

import java.util.List;

public interface WindowEngine {

  List<WindowEvent> process(Object data, int startRow, int endRow);

  List<WindowEvent> forceClose();

  static WindowEngine create(StreamWindow window) {
    switch (window.getType()) {
      case PERIOD:
        return new PeriodWindowEngine();
      case TUMBLE:
        return new TumbleWindowEngine();
      case HOP:
        return new HopWindowEngine();
      case VARIATION:
        return new VariationWindowEngine();
      case CAPACITY:
        return new CapacityWindowEngine();
      case ASOF:
        return new AsofWindowEngine();
      default:
        throw new UnsupportedOperationException("Unsupported window type: " + window.getType());
    }
  }
}
