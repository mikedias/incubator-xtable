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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Properties;

import org.junit.jupiter.api.Test;

import org.apache.xtable.paimon.PaimonSourceConfig.PaimonEmitFilesMode;

public class TestPaimonSourceConfig {

  @Test
  void testDefaultIsData() {
    PaimonSourceConfig cfg = PaimonSourceConfig.fromProperties(new Properties());
    assertEquals(PaimonEmitFilesMode.DATA, cfg.getEmitFilesMode());
  }

  @Test
  void testParseChangelog() {
    Properties props = new Properties();
    props.setProperty(PaimonSourceConfig.EMIT_FILES_MODE, "changelog");
    PaimonSourceConfig cfg = PaimonSourceConfig.fromProperties(props);
    assertEquals(PaimonEmitFilesMode.CHANGELOG, cfg.getEmitFilesMode());
  }

  @Test
  void testInvalidValue() {
    Properties props = new Properties();
    props.setProperty(PaimonSourceConfig.EMIT_FILES_MODE, "wat");
    assertThrows(IllegalArgumentException.class, () -> PaimonSourceConfig.fromProperties(props));
  }
}
