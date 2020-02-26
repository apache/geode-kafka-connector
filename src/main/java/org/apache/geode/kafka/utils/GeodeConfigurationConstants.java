package org.apache.geode.kafka.utils;

import org.apache.kafka.common.config.types.Password;

public class GeodeConfigurationConstants {
  /**
   * GEODE SPECIFIC CONFIGURATION
   */
  //Identifier for each task
  public static final String TASK_ID = "GEODE_TASK_ID"; // One config per task
  //Specifies which Locators to connect to Apache Geode
  public static final String LOCATORS = "locators";
  public static final String DEFAULT_LOCATOR = "localhost[10334]";
  public static final String SECURITY_CLIENT_AUTH_INIT = "security-client-auth-init";
  public static final Password DEFAULT_SECURITY_AUTH_INIT =
      new Password("org.apache.geode.kafka.security.SystemPropertyAuthInit");
  public static final String SECURITY_USER = "security-username";
  public static final String SECURITY_PASSWORD = "security-password";
  public static final String TASK_ID_DOCUMENTATION = "Internally used to identify each task";
  public static final String
      LOCATORS_DOCUMENTATION =
      "A comma separated string of locators that configure which locators to connect to";
  public static final String
      SECURITY_USER_DOCUMENTATION =
      "Supply a username to be used to authenticate with Geode.  Will automatically set the security-client-auth-init to use a SystemPropertyAuthInit if one isn't supplied by the user";
  public static final String SECURITY_PASSWORD_DOCUMENTATION = "Supply a password to be used to authenticate with Geode";
  public static final String
      SECURITY_CLIENT_AUTH_INIT_DOCUMENTATION =
      "Point to the Java class that implements the [AuthInitialize Interface](https://geode.apache.org/docs/guide/19/managing/security/implementing_authentication.html)";
}
