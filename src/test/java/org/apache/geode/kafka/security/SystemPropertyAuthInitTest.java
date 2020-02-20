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
package org.apache.geode.kafka.security;

import static org.apache.geode.kafka.GeodeConnectorConfig.SECURITY_PASSWORD;
import static org.apache.geode.kafka.GeodeConnectorConfig.SECURITY_USER;
import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.security.AuthInitialize;

public class SystemPropertyAuthInitTest {

  @Test
  public void userNameAndPasswordAreObtainedFromSecurityProps() {
    SystemPropertyAuthInit auth = new SystemPropertyAuthInit();
    String userName = "someUsername";
    String password = "somePassword";

    Properties securityProps = new Properties();
    securityProps.put(SECURITY_USER, userName);
    securityProps.put(SECURITY_PASSWORD, password);
    Properties credentials = auth.getCredentials(securityProps, null, true);
    assertEquals(credentials.get((AuthInitialize.SECURITY_USERNAME)), userName);
    assertEquals(credentials.get((AuthInitialize.SECURITY_PASSWORD)), password);
  }
}
