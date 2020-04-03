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
package org.apache.geode.kafka.converter;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;

public class JsonPdxConverter implements Converter {
  public static final String JSON_TYPE_ANNOTATION = "\"@type\"";
  // Default value = false
  public static final String ADD_TYPE_ANNOTATION_TO_JSON = "add-type-annotation-to-json";
  final private Map<String, String> internalConfig = new HashMap<>();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    if (configs != null) {
      configs.forEach((key, value) -> internalConfig.put(key, (String) value));
    }
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    PdxInstance pdxInstanceValue = (PdxInstance) value;
    byte[] jsonBytes = getJsonBytes(pdxInstanceValue);
    if (!shouldAddTypeAnnotation()) {
      return jsonBytes;
    }
    String jsonString = new String(jsonBytes);
    if (!jsonString.contains(JSON_TYPE_ANNOTATION)) {
      jsonString = jsonString.replaceFirst("\\{",
          "{" + JSON_TYPE_ANNOTATION + " : \"" + pdxInstanceValue.getClassName() + "\",");
    }
    return jsonString.getBytes();
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    return new SchemaAndValue(null, JSONFormatter.fromJSON(value));
  }

  byte[] getJsonBytes(PdxInstance pdxInstanceValue) {
    return JSONFormatter.toJSONByteArray(pdxInstanceValue);
  }

  boolean shouldAddTypeAnnotation() {
    return Boolean.parseBoolean(internalConfig.get(ADD_TYPE_ANNOTATION_TO_JSON));
  }

  public Map<String, String> getInternalConfig() {
    return internalConfig;
  }
}
