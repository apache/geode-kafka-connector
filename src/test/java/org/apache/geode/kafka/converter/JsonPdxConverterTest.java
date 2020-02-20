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

import static org.apache.geode.kafka.converter.JsonPdxConverter.JSON_TYPE_ANNOTATION;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.pdx.PdxInstance;

public class JsonPdxConverterTest {
  private JsonPdxConverter spyConverter;
  PdxInstance mockPdxInstance;
  private final String className = "className";
  private final String jsonWithoutTypeAnnotation = "{\"name\" : \"test\"}";
  private final String jsonWithTypeAnnotation =
      "{" + JSON_TYPE_ANNOTATION + " : \"" + className + "\",\"name\" : \"test\"}";

  @Before
  public void setUp() {
    spyConverter = spy(new JsonPdxConverter());
    mockPdxInstance = mock(PdxInstance.class);
    when(mockPdxInstance.getClassName()).thenReturn(className);
  }

  @Test
  public void configurePopulatesInternalConfiguration() {
    Map<String, String> map = new HashMap<>();
    IntStream.range(0, 10).forEach(i -> map.put("key" + i, "value" + i));
    spyConverter.configure(map, false);

    assertThat(spyConverter.getInternalConfig(), is(map));
  }

  @Test
  public void fromConnectDataAddsTypeAnnotationWhenConfigurationIsSetAndJsonDoesNotAlreadyContainAnnotation() {
    byte[] jsonBytes = jsonWithoutTypeAnnotation.getBytes();

    doReturn(jsonBytes).when(spyConverter).getJsonBytes(mockPdxInstance);
    doReturn(true).when(spyConverter).shouldAddTypeAnnotation();

    byte[] expectedOutputJsonBytes = jsonWithTypeAnnotation.getBytes();

    assertThat(spyConverter.fromConnectData(null, null, mockPdxInstance),
        is(expectedOutputJsonBytes));
  }

  @Test
  public void fromConnectDataDoesNotAddTypeAnnotationWhenConfigurationIsSetAndJsonAlreadyContainsAnnotation() {
    byte[] jsonBytes = jsonWithTypeAnnotation.getBytes();

    doReturn(jsonBytes).when(spyConverter).getJsonBytes(mockPdxInstance);
    doReturn(true).when(spyConverter).shouldAddTypeAnnotation();

    assertThat(spyConverter.fromConnectData(null, null, mockPdxInstance), is(jsonBytes));
  }

  @Test
  public void fromConnectDataDoesNotAddTypeAnnotationWhenConfigurationIsNotSet() {
    byte[] jsonBytes = jsonWithoutTypeAnnotation.getBytes();

    doReturn(jsonBytes).when(spyConverter).getJsonBytes(mockPdxInstance);
    doReturn(false).when(spyConverter).shouldAddTypeAnnotation();

    assertThat(spyConverter.fromConnectData(null, null, mockPdxInstance), is(jsonBytes));
  }
}
