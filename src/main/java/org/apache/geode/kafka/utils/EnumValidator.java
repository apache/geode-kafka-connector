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
package org.apache.geode.kafka.utils;

import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class EnumValidator implements ConfigDef.Validator {
  private final Set<String> validValues;

  public EnumValidator(Set<String> validValues) {
    this.validValues = validValues;
  }

  public static <T> EnumValidator in(T[] enumerators) {
    Set<String> validValues = new HashSet<>(enumerators.length);
    for (T e : enumerators) {
      validValues.add(e.toString().toLowerCase());
    }
    return new EnumValidator(validValues);
  }

  @Override
  public void ensureValid(String key, Object value) {
    if (!validValues.contains(value.toString().toLowerCase())) {
      throw new ConfigException(key, value, "Invalid enumeration value");
    }

  }
}
