package geode.kafka;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.List;

public class GeodeContext {

    private ClientCache clientCache;


    public GeodeContext(GeodeConnectorConfig connectorConfig) {
        clientCache = createClientCache(connectorConfig.getLocatorHostPorts(), connectorConfig.getDurableClientId(), connectorConfig.getDurableClientTimeout());
    }

    public ClientCache getClientCache() {
        return clientCache;
    }

    public ClientCache createClientCache(List<LocatorHostPort> locators, String durableClientName, String durableClientTimeOut) {
        ClientCacheFactory ccf = new ClientCacheFactory().set("durable-client-id", durableClientName)
                .set("durable-client-timeout", durableClientTimeOut)
                .setPoolSubscriptionEnabled(true);
        for (LocatorHostPort locator: locators) {
            ccf.addPoolLocator(locator.getHostName(), locator.getPort()).create();
        }
        return ccf.create();
    }

    public CqQuery newCq(String name, String query, CqAttributes cqAttributes, boolean isDurable) throws ConnectException {
        try {
            CqQuery cq = clientCache.getQueryService().newCq(name, query, cqAttributes, isDurable);
            cq.execute();
            return cq;
        } catch (RegionNotFoundException | CqException | CqExistsException e) {
            e.printStackTrace();
            throw new ConnectException(e);
        }
    }
}
