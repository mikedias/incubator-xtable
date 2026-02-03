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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package org.apache.xtable.paimon;

import java.util.Properties;

import lombok.Value;

/** Configuration of Paimon source format for the sync process. */
@Value
public class PaimonSourceConfig {

  public static final String EMIT_FILES_MODE = "xtable.paimon.source.emit_files";

  /** Enum controlling whether Paimon conversion operates on data files or changelog files. */
  public enum PaimonEmitFilesMode {
    DATA,
    CHANGELOG;
  }

  PaimonEmitFilesMode emitFilesMode;

  public static PaimonSourceConfig fromProperties(Properties properties) {
    if (properties == null) {
      return new PaimonSourceConfig(PaimonEmitFilesMode.DATA);
    }
    String rawValue = properties.getProperty(EMIT_FILES_MODE, "DATA");
    return new PaimonSourceConfig(PaimonEmitFilesMode.valueOf(rawValue.toUpperCase()));
  }
}
