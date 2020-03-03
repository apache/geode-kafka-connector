/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.kafka;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Version {

  private static final Logger log = LoggerFactory.getLogger(Version.class);
  private static String version = "unknown";

  private static final String VERSION_FILE = "/kafka-connect-geode-version.properties";

  static {
    try {
      Properties properties = new Properties();
      try (InputStream stream = Version.class.getResourceAsStream(VERSION_FILE)) {
        properties.load(stream);
        version = properties.getProperty("version", version).trim();
      }

    } catch (Exception exception) {
      log.warn("Error while loading version");
    }
  }

  public static String getVersion() {
    return version;
  }


}
