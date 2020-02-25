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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.storage.StringConverter;

import org.apache.geode.annotations.VisibleForTesting;

public class GeodeConnectorConfig extends AbstractConfig {

  // GeodeKafka Specific Configuration
  /**
   * Identifier for each task
   */
  public static final String TASK_ID = "GEODE_TASK_ID"; // One config per task
  /**
   * Specifies which Locators to connect to Apache Geode
   */
  private static final String LOCATORS = "locators";
  private static final String DEFAULT_LOCATOR = "localhost[10334]";
  private static final String SECURITY_CLIENT_AUTH_INIT = "security-client-auth-init";
  private static final String DEFAULT_SECURITY_AUTH_INIT =
      "org.apache.geode.kafka.security.SystemPropertyAuthInit";
  private static final String SECURITY_USER = "security-username";
  private static final String SECURITY_PASSWORD = "security-password";
  private static final String TASK_ID_DOCUMENTATION = "Internally used to identify each task";
  private static final String
      LOCATORS_DOCUMENTATION =
      "A comma separated string of locators that configure which locators to connect to";
  private static final String
      SECURITY_USER_DOCUMENTATION =
      "Supply a username to be used to authenticate with Geode.  Will automatically set the security-client-auth-init to use a SystemPropertyAuthInit if one isn't supplied by the user";
  private static final String SECURITY_PASSWORD_DOCUMENTATION = "Supply a password to be used to authenticate with Geode";
  private static final String
      SECURITY_CLIENT_AUTH_INIT_DOCUMENTATION =
      "Point to the Java class that implements the [AuthInitialize Interface](https://geode.apache.org/docs/guide/19/managing/security/implementing_authentication.html)";

  public static final String DEFAULT_KEY_CONVERTER = StringConverter.class.getCanonicalName();
  public static final String DEFAULT_VALUE_CONVERTER = StringConverter.class.getCanonicalName();

  protected final int taskId;
  protected List<LocatorHostPort> locatorHostPorts;
  private String securityClientAuthInit;
  private String securityUserName;
  private String securityPassword;

  @VisibleForTesting
  protected GeodeConnectorConfig() {
    super(new ConfigDef(), new HashMap<>());
    taskId = 0;
  }

  public GeodeConnectorConfig(ConfigDef configDef, Map<String, String> connectorProperties) {
    super(configDef, connectorProperties);
    taskId = getInt(TASK_ID);
    locatorHostPorts = parseLocators(getString(GeodeConnectorConfig.LOCATORS));
    securityUserName = getString(SECURITY_USER);
    securityPassword = getString(SECURITY_PASSWORD);
    securityClientAuthInit = getString(SECURITY_CLIENT_AUTH_INIT);
    // if we registered a username/password instead of auth init, we should use the default auth
    // init if one isn't specified
    if (usesSecurity()) {
      securityClientAuthInit =
          securityClientAuthInit != null ? securityClientAuthInit : DEFAULT_SECURITY_AUTH_INIT;
    }
  }

  protected static ConfigDef configurables() {
    ConfigDef configDef = new ConfigDef();
    configDef.define(
        TASK_ID,
        ConfigDef.Type.INT,
        "0",
        ConfigDef.Importance.MEDIUM,
        TASK_ID_DOCUMENTATION);
    configDef.define(
        LOCATORS,
        ConfigDef.Type.STRING,
        DEFAULT_LOCATOR,
        ConfigDef.Importance.HIGH,
        LOCATORS_DOCUMENTATION);
    configDef.define(
        SECURITY_USER,
        ConfigDef.Type.STRING,
        null,
        ConfigDef.Importance.HIGH,
        SECURITY_USER_DOCUMENTATION);

    configDef.define(
        SECURITY_PASSWORD,
        ConfigDef.Type.PASSWORD,
        null,
        ConfigDef.Importance.HIGH,
        SECURITY_PASSWORD_DOCUMENTATION);

    configDef.define(
        SECURITY_CLIENT_AUTH_INIT,
        ConfigDef.Type.PASSWORD,
        null,
        ConfigDef.Importance.HIGH,
        SECURITY_CLIENT_AUTH_INIT_DOCUMENTATION);
    return configDef;
  }


  public static Map<String, List<String>> parseTopicToRegions(String combinedBindings) {
    // It's the same formatting, so parsing is the same going topic to region or region to topic
    return parseRegionToTopics(combinedBindings);
  }

  /**
   * Given a string of the form [region:topic,...] will produce a map where the key is the
   * regionName and the value is a list of topicNames to push values to
   *
   * @param combinedBindings a string with similar form to "[region:topic,...], [region2:topic2,...]
   * @return mapping of regionName to list of topics to update
   */
  public static Map<String, List<String>> parseRegionToTopics(String combinedBindings) {
    if (combinedBindings == null || combinedBindings.equals("")) {
      return new HashMap<>();
    }
    List<String> bindings = parseBindings(combinedBindings);
    return bindings.stream().map(
        GeodeConnectorConfig::parseBinding)
        .collect(Collectors.toMap(regionToTopicsArray -> regionToTopicsArray[0],
            regionToTopicsArray -> parseStringByComma(regionToTopicsArray[1])));
  }

  public static List<String> parseBindings(String bindings) {
    return Arrays.stream(bindings.split("](\\s)*,")).map((s) -> {
      s = s.replaceAll("\\[", "");
      s = s.replaceAll("]", "");
      s = s.trim();
      return s;
    }).collect(Collectors.toList());
  }

  private static String[] parseBinding(String binding) {
    return binding.split(":");
  }

  // Used to parse a string of topics or regions
  public static List<String> parseStringByComma(String string) {
    return parseStringBy(string, ",");
  }

  public static List<String> parseStringBy(String string, String regex) {
    return Arrays.stream(string.split(regex)).map(String::trim).collect(Collectors.toList());
  }

  public static String reconstructString(Collection<String> strings) {
    return String.join("],[", strings);
  }

  List<LocatorHostPort> parseLocators(String locators) {
    return Arrays.stream(locators.split(",")).map((s) -> {
      String locatorString = s.trim();
      return parseLocator(locatorString);
    }).collect(Collectors.toList());
  }

  private LocatorHostPort parseLocator(String locatorString) {
    String[] splits = locatorString.split("\\[");
    String locator = splits[0];
    int port = Integer.parseInt(splits[1].replace("]", ""));
    return new LocatorHostPort(locator, port);
  }

  public int getTaskId() {
    return taskId;
  }

  public List<LocatorHostPort> getLocatorHostPorts() {
    return locatorHostPorts;
  }

  public String getSecurityClientAuthInit() {
    return securityClientAuthInit;
  }

  public String getSecurityUserName() {
    return securityUserName;
  }

  public String getSecurityPassword() {
    return securityPassword;
  }

  public boolean usesSecurity() {
    return securityClientAuthInit != null || securityUserName != null;
  }
}
