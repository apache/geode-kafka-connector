package org.geode.kafka.security;

import org.apache.geode.security.AuthInitialize;
import org.junit.Test;

import java.util.Properties;

import static org.geode.kafka.GeodeConnectorConfig.SECURITY_PASSWORD;
import static org.geode.kafka.GeodeConnectorConfig.SECURITY_USER;
import static org.junit.Assert.assertEquals;

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
