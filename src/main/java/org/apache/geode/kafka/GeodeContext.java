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

import static org.apache.geode.kafka.utils.GeodeConfigurationConstants.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.kafka.utils.GeodeConfigurationConstants.SECURITY_PASSWORD;
import static org.apache.geode.kafka.utils.GeodeConfigurationConstants.SECURITY_USER;

import java.util.List;

import org.apache.kafka.connect.errors.ConnectException;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.CqResults;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeodeContext {

  private ClientCache clientCache;
  private static final Logger logger = LoggerFactory.getLogger(GeodeContext.class);


  public GeodeContext() {}

  public ClientCache connectClient(List<LocatorHostPort> locatorHostPortList,
      String durableClientId, String durableClientTimeout, String securityAuthInit,
      String securityUserName, String securityPassword, boolean usesSecurity) {
    clientCache = createClientCache(locatorHostPortList, durableClientId, durableClientTimeout,
        securityAuthInit, securityUserName, securityPassword, usesSecurity);
    return clientCache;
  }

  public ClientCache connectClient(List<LocatorHostPort> locatorHostPortList,
      String securityAuthInit, String securityUserName, String securityPassword,
      boolean usesSecurity) {
    clientCache = createClientCache(locatorHostPortList, "", "", securityAuthInit, securityUserName,
        securityPassword, usesSecurity);
    return clientCache;
  }

  public ClientCache getClientCache() {
    return clientCache;
  }

  public ClientCache createClientCache(List<LocatorHostPort> locators, String durableClientName,
      String durableClientTimeOut, String securityAuthInit, String securityUserName,
      String securityPassword, boolean usesSecurity) {
    try {
      ClientCacheFactory ccf = new ClientCacheFactory();

      ccf.setPdxReadSerialized(true);
      if (usesSecurity) {
        if (securityUserName != null && securityPassword != null) {
          ccf.set(SECURITY_USER, securityUserName);
          ccf.set(SECURITY_PASSWORD, securityPassword);
        }
        ccf.set(SECURITY_CLIENT_AUTH_INIT, securityAuthInit);
      }
      if (!durableClientName.equals("")) {
        ccf.set("durable-client-id", durableClientName)
            .set("durable-client-timeout", durableClientTimeOut);
      }
      // currently we only allow using the default pool.
      // If we ever want to allow adding multiple pools we'll have to configure pool factories
      ccf.setPoolSubscriptionEnabled(true);

      for (LocatorHostPort locator : locators) {
        ccf.addPoolLocator(locator.getHostName(), locator.getPort());
      }
      return ccf.create();
    } catch (Exception e) {
      throw new ConnectException(
          "Unable to create an client cache connected to Apache Geode cluster");
    }
  }

  public CqQuery newCq(String name, String query, CqAttributes cqAttributes, boolean isDurable)
      throws ConnectException {
    try {
      CqQuery cq = clientCache.getQueryService().newCq(name, query, cqAttributes, isDurable);
      cq.execute();
      return cq;
    } catch (RegionNotFoundException | CqException | CqExistsException e) {
      throw new ConnectException(e);
    }
  }

  public <E> CqResults<E> newCqWithInitialResults(String name, String query,
      CqAttributes cqAttributes,
      boolean isDurable) throws ConnectException {
    try {
      CqQuery cq = clientCache.getQueryService().newCq(name, query, cqAttributes, isDurable);
      return cq.executeWithInitialResults();
    } catch (RegionNotFoundException | CqException | CqExistsException e) {
      throw new ConnectException(e);
    }
  }

  public void close(boolean keepAlive) {
    clientCache.close(keepAlive);
  }
}
