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
package geode.kafka.example;

import java.util.Properties;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.security.AuthInitialize;
import org.apache.geode.security.AuthenticationFailedException;

/**
 * This is purely for example purposes and used in conjunction with the SimpleSecurityManager in Apache Geode
 * DO NOT USE THIS AS A REAL WORLD SOLUTION
 */
public class ExampleAuthInit implements AuthInitialize {
  @Override
  public Properties getCredentials(Properties securityProps, DistributedMember server,
      boolean isPeer) throws AuthenticationFailedException {
    Properties extractedProperties = new Properties();
    // Do not do this in real use case. This is hardcoded and sets the user name and password for
    // all users
    extractedProperties.put("security-username", "Bearer");
    extractedProperties.put("security-password", "Bearer");
    return extractedProperties;
  }
}
