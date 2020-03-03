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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

public class EnumRecommender implements ConfigDef.Recommender {
  private final List<String> valuesList;

  public EnumRecommender(List<String> valuesList) {
    this.valuesList = valuesList;
  }

  public static <T> EnumRecommender in(T[] enumerators) {
    final List<String> valuesList = new ArrayList<>(enumerators.length);
    for (T value : enumerators) {
      valuesList.add(value.toString());
    }
    return new EnumRecommender(valuesList);
  }

  @Override
  public List<Object> validValues(String s, Map<String, Object> map) {
    return new ArrayList<>(valuesList);
  }

  @Override
  public String toString() {
    return valuesList.toString();
  }

  @Override
  public boolean visible(String s, Map<String, Object> map) {
    return true;
  }
}
