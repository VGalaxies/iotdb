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

package org.apache.iotdb.streamnode.service;

import org.apache.iotdb.calc.service.AbstractTemporaryQueryDataFileService;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;

import java.io.File;

public class StreamNodeTemporaryQueryDataFileService extends AbstractTemporaryQueryDataFileService {

  private static final String TEMPORARY_FILE_DIR =
      StreamNodeDescriptor.getInstance().getConfig().getSystemDir()
          + File.separator
          + "udf"
          + File.separator
          + "tmp";

  @Override
  protected String getTemporaryFileDir() {
    return TEMPORARY_FILE_DIR;
  }

  public static StreamNodeTemporaryQueryDataFileService getInstance() {
    return StreamNodeTemporaryQueryDataFileServiceHelper.INSTANCE;
  }

  private static class StreamNodeTemporaryQueryDataFileServiceHelper {

    private static final StreamNodeTemporaryQueryDataFileService INSTANCE =
        new StreamNodeTemporaryQueryDataFileService();

    private StreamNodeTemporaryQueryDataFileServiceHelper() {}
  }
}
