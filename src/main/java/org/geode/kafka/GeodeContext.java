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
package org.geode.kafka;

import java.util.Collection;
import java.util.List;

import org.apache.kafka.connect.errors.ConnectException;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.RegionNotFoundException;

import static org.geode.kafka.GeodeConnectorConfig.SECURITY_CLIENT_AUTH_INIT;

public class GeodeContext {

  private ClientCache clientCache;


  public GeodeContext() {}

  public ClientCache connectClient(List<LocatorHostPort> locatorHostPortList,
      String durableClientId, String durableClientTimeout, String securityAuthInit, boolean usesSecurity) {
    clientCache = createClientCache(locatorHostPortList, durableClientId, durableClientTimeout,
        securityAuthInit, usesSecurity);
    return clientCache;
  }

  public ClientCache connectClient(List<LocatorHostPort> locatorHostPortList,
      String securityAuthInit, boolean usesSecurity) {
    clientCache = createClientCache(locatorHostPortList, "", "", securityAuthInit, usesSecurity);
    return clientCache;
  }

  public ClientCache getClientCache() {
    return clientCache;
  }

  public ClientCache createClientCache(List<LocatorHostPort> locators, String durableClientName,
      String durableClientTimeOut, String securityAuthInit, boolean usesSecurity) {
    ClientCacheFactory ccf = new ClientCacheFactory();

    if (usesSecurity ) {
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

  public Collection newCqWithInitialResults(String name, String query, CqAttributes cqAttributes,
      boolean isDurable) throws ConnectException {
    try {
      CqQuery cq = clientCache.getQueryService().newCq(name, query, cqAttributes, isDurable);
      return cq.executeWithInitialResults();
    } catch (RegionNotFoundException | CqException | CqExistsException e) {
      throw new ConnectException(e);
    }
  }
}
