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


import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class GeodeConnectorConfigTest {

  @Test
  public void parseRegionNamesShouldSplitOnComma() {
    GeodeConnectorConfig config = new GeodeConnectorConfig();
    List<String> regionNames = config.parseStringByComma("region1,region2,region3,region4");
    assertEquals(4, regionNames.size());
    assertThat(true, allOf(is(regionNames.contains("region1")), is(regionNames.contains("region2")),
        is(regionNames.contains("region3")), is(regionNames.contains("region4"))));
  }

  @Test
  public void parseRegionNamesShouldChomp() {
    GeodeConnectorConfig config = new GeodeConnectorConfig();
    List<String> regionNames = config.parseStringByComma("region1, region2, region3,region4");
    assertEquals(4, regionNames.size());
    assertThat(true,
        allOf(is(regionNames.contains("region1")),
            is(regionNames.contains("region2")), is(regionNames.contains("region3")),
            is(regionNames.contains("region4"))));
  }

  @Test
  public void shouldBeAbleToParseGeodeLocatorStrings() {
    GeodeConnectorConfig config = new GeodeConnectorConfig();
    String locatorString = "localhost[8888], localhost[8881]";
    List<LocatorHostPort> locators = config.parseLocators(locatorString);
    assertThat(2, is(locators.size()));
  }

  @Test
  @Parameters(method = "oneToOneBindings")
  public void parseBindingsCanSplitOneToOneBindings(String value) {
    List<String> splitBindings = GeodeConnectorConfig.parseBindings(value);
    assertEquals(2, splitBindings.size());
  }

  @Test
  public void parseBindingsCanSplitASingleOneToOneBindings() {
    String binding = "[region1:topic1]";
    List<String> splitBindings = GeodeConnectorConfig.parseBindings(binding);
    assertEquals(1, splitBindings.size());
    assertEquals(binding.replaceAll("\\[", "").replaceAll("]", ""), splitBindings.get(0));
  }

  public List<String> oneToOneBindings() {
    return Arrays.asList(
        "[region1:topic1],[region2:topic2]", "[region1:topic1] , [region2:topic2]",
        "[region1:topic1], [region2:topic2] ,", "[region1: topic1], [region2 :topic2]");
  }

  @Test
  @Parameters(method = "oneToManyBindings")
  public void parseBindingsCanSplitOneToManyBindings(String value) {
    List<String> splitBindings = GeodeConnectorConfig.parseBindings(value);
    assertEquals(Arrays.toString(splitBindings.toArray()), 2, splitBindings.size());
  }

  public List<String> oneToManyBindings() {
    return Arrays.asList("[region1:topic1,topic2],[region2:topic2,topic3]",
        "[region1:topic1 , topic2] , [region2:topic2 , topic3]",
        "[region1:topic1 ,], [region2:topic2 ,] ,",
        "[region1: topic1 ,topic3], [region2 :topic2]");
  }

  @Test
  @Parameters(method = "oneToManyBindings")
  public void reconstructBindingsToStringShouldReformAParsableString(String value) {
    List<String> splitBindings = GeodeConnectorConfig.parseBindings(value);
    String reconstructString = GeodeConnectorConfig.reconstructString(splitBindings);
    splitBindings = GeodeConnectorConfig.parseBindings(reconstructString);
    assertEquals(Arrays.toString(splitBindings.toArray()), 2, splitBindings.size());
    for (String topicOrRegion : splitBindings) {
      assertFalse(topicOrRegion.contains("\\["));
      assertFalse(topicOrRegion.contains("\\]"));
    }
  }

  @Test
  @Parameters(method = "oneToOneBindings")
  public void configurationShouldReturnRegionToTopicsMappingWhenParseRegionToTopics(String value) {
    Map<String, List<String>> regionToTopics = GeodeConnectorConfig.parseRegionToTopics(value);
    assertEquals(2, regionToTopics.size());
    assertTrue(regionToTopics.get("region1") != null);
    assertEquals(1, regionToTopics.get("region1").size());
    assertTrue(regionToTopics.get("region1").contains("topic1"));
  }

  @Test
  public void regionToTopicParsingShouldParseCorrectlyWithASingleBinding() {
    Map<String, List<String>> regionToTopics =
        GeodeConnectorConfig.parseRegionToTopics("[region1:topic1]");
    assertTrue(regionToTopics.get("region1") != null);
    assertEquals(1, regionToTopics.get("region1").size());
    assertTrue(regionToTopics.get("region1").contains("topic1"));
  }

  @Test
  public void usesSecurityShouldBeTrueIfSecurityUserSet() {
    Map<String, String> props = new HashMap<>();
    props.put(GeodeConnectorConfig.SECURITY_USER, "some user");
    GeodeConnectorConfig config =
        new GeodeConnectorConfig(GeodeConnectorConfig.configurables(), props);
    assertTrue(config.usesSecurity());
  }

  @Test
  public void usesSecurityShouldBeTrueIfSecurityClientAuthInitSet() {
    Map<String, String> props = new HashMap<>();
    props.put(GeodeConnectorConfig.SECURITY_CLIENT_AUTH_INIT, "some_class");
    GeodeConnectorConfig config =
        new GeodeConnectorConfig(GeodeConnectorConfig.configurables(), props);
    assertTrue(config.usesSecurity());
  }

  @Test
  public void usesSecurityShouldBeFalseIfSecurityUserAndSecurityClientAuthInitNotSet() {
    Map<String, String> props = new HashMap<>();
    GeodeConnectorConfig config =
        new GeodeConnectorConfig(GeodeConnectorConfig.configurables(), props);
    assertFalse(config.usesSecurity());
  }

  @Test
  public void securityClientAuthInitShouldBeSetIfUserIsSet() {
    Map<String, String> props = new HashMap<>();
    props.put(GeodeConnectorConfig.SECURITY_USER, "some user");
    GeodeConnectorConfig config =
        new GeodeConnectorConfig(GeodeConnectorConfig.configurables(), props);
    assertNotNull(config.getSecurityClientAuthInit());
  }

  @Test
  public void securityClientAuthInitShouldNotBeSetIfUserIsNotSetAndNotSpecificallySet() {
    Map<String, String> props = new HashMap<>();
    GeodeConnectorConfig config =
        new GeodeConnectorConfig(GeodeConnectorConfig.configurables(), props);
    assertNull(config.getSecurityClientAuthInit());
  }

  /*
   * taskId = Integer.parseInt(connectorProperties.get(TASK_ID));
   * durableClientIdPrefix = connectorProperties.get(DURABLE_CLIENT_ID_PREFIX);
   * if (isDurable(durableClientIdPrefix)) {
   * durableClientId = durableClientIdPrefix + taskId;
   * } else {
   * durableClientId = "";
   * }
   * durableClientTimeout = connectorProperties.get(DURABLE_CLIENT_TIME_OUT);
   * regionToTopics =
   * parseRegionToTopics(connectorProperties.get(GeodeConnectorConfig.REGION_TO_TOPIC_BINDINGS));
   * topicToRegions =
   * parseTopicToRegions(connectorProperties.get(GeodeConnectorConfig.REGION_TO_TOPIC_BINDINGS));
   * locatorHostPorts = parseLocators(connectorProperties.get(GeodeConnectorConfig.LOCATORS));
   *
   */



}
