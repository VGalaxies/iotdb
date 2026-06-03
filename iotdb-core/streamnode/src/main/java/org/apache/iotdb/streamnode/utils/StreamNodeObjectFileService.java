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

package org.apache.iotdb.streamnode.utils;

import org.apache.iotdb.calc.utils.IObjectFileService;

import org.apache.tsfile.utils.Binary;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Optional;

public class StreamNodeObjectFileService implements IObjectFileService {

  public static final StreamNodeObjectFileService INSTANCE = new StreamNodeObjectFileService();

  private StreamNodeObjectFileService() {}

  @Override
  public ByteBuffer readObjectContent(
      String relativePath, long offset, int readSize, boolean mayNotInCurrentNode) {
    throw new UnsupportedOperationException("readObjectContent is not supported in StreamNode");
  }

  @Override
  public Optional<File> getObjectPathFromBinary(Binary binary, boolean needTempFile) {
    throw new UnsupportedOperationException(
        "getObjectPathFromBinary is not supported in StreamNode");
  }

  @Override
  public void deleteObjectPathFromBinary(Binary binary) {
    throw new UnsupportedOperationException(
        "deleteObjectPathFromBinary is not supported in StreamNode");
  }

  @Override
  public void deleteObjectPath(File file) {
    throw new UnsupportedOperationException("deleteObjectPath is not supported in StreamNode");
  }
}
