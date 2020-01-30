package geode.kafka;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.Collection;
import java.util.List;

public class GeodeContext {

    private ClientCache clientCache;


    public GeodeContext() {
    }

    public ClientCache connectClient(List<LocatorHostPort> locatorHostPortList, String durableClientId, String durableClientTimeout) {
        clientCache = createClientCache(locatorHostPortList, durableClientId, durableClientTimeout);
        return clientCache;
    }

    public ClientCache connectClient(List<LocatorHostPort> locatorHostPortList) {
        clientCache = createClientCache(locatorHostPortList, "", "");
        return clientCache;
    }

    public ClientCache getClientCache() {
        return clientCache;
    }

    /**
     *
     * @param locators
     * @param durableClientName
     * @param durableClientTimeOut
     * @return
     */
    public ClientCache createClientCache(List<LocatorHostPort> locators, String durableClientName, String durableClientTimeOut) {
        ClientCacheFactory ccf = new ClientCacheFactory();
        if (!durableClientName.equals("")) {
            ccf.set("durable-client-id", durableClientName)
                    .set("durable-client-timeout", durableClientTimeOut);
        }
        //currently we only allow using the default pool.
        //If we ever want to allow adding multiple pools we'll have to configure pool factories
        ccf.setPoolSubscriptionEnabled(true);

        for (LocatorHostPort locator: locators) {
            ccf.addPoolLocator(locator.getHostName(), locator.getPort());
        }
        return ccf.create();
    }

    public CqQuery newCq(String name, String query, CqAttributes cqAttributes, boolean isDurable) throws ConnectException {
        try {
            CqQuery cq = clientCache.getQueryService().newCq(name, query, cqAttributes, isDurable);
            cq.execute();
            return cq;
        } catch (RegionNotFoundException | CqException | CqExistsException e) {
            throw new ConnectException(e);
        }
    }

    public Collection newCqWithInitialResults(String name, String query, CqAttributes cqAttributes, boolean isDurable) throws ConnectException {
        try {
            CqQuery cq = clientCache.getQueryService().newCq(name, query, cqAttributes, isDurable);
            return cq.executeWithInitialResults();
        } catch (RegionNotFoundException | CqException | CqExistsException e) {
            throw new ConnectException(e);
        }
    }
}
