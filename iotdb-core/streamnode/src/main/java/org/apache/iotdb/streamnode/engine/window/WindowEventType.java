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

/**
 * Type of a window event.
 *
 * <ul>
 *   <li>{@link #WINDOW_OPEN}: window is open / producing intermediate results
 *   <li>{@link #WINDOW_CLOSE}: window is logically closed; may still recompute before destroy
 *   <li>{@link #WINDOW_DESTROY}: window ends; sink may advance source commit after flush (eof)
 * </ul>
 */
public enum WindowEventType {
  WINDOW_OPEN,
  WINDOW_CLOSE,
  WINDOW_DESTROY
}
