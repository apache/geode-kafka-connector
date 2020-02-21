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
package org.apache.geode.kafka.utilities;

import java.util.List;
import java.util.Objects;

public class TestObject {
  public String name;
  public int age;
  public double number;
  public List<String> words;

  @SuppressWarnings("unused")
  public TestObject() {}

  public TestObject(String name, int age, double number, List<String> words) {
    this.name = name;
    this.age = age;
    this.number = number;
    this.words = words;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TestObject that = (TestObject) o;
    return age == that.age &&
        Double.compare(that.number, number) == 0 &&
        Objects.equals(name, that.name) &&
        Objects.equals(words, that.words);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, age, number, words);
  }
}
