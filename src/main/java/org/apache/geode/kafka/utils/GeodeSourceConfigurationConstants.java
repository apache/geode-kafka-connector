package org.apache.geode.kafka.utils;

public class GeodeSourceConfigurationConstants {
  /**
   * SOURCE SPECIFIC CONFIGURATIONS
   */
  // Geode Configuration
  public static final String DURABLE_CLIENT_ID_PREFIX = "durable-client-id-prefix";
  public static final String DEFAULT_DURABLE_CLIENT_ID = "";
  public static final String DURABLE_CLIENT_TIME_OUT = "durable-client-timeout";
  public static final String DEFAULT_DURABLE_CLIENT_TIMEOUT = "60000";
  public static final String CQ_PREFIX = "cq-prefix";
  public static final String DEFAULT_CQ_PREFIX = "cqForGeodeKafka";
  //Used as a key for source partitions
  public static final String REGION_PARTITION = "regionPartition";
  public static final String REGION_TO_TOPIC_BINDINGS = "region-to-topics";
  public static final String DEFAULT_REGION_TO_TOPIC_BINDING = "[gkcRegion:gkcTopic]";
  public static final String CQS_TO_REGISTER = "cqsToRegister"; // used internally so that only 1
  // task will register a cq
  public static final String BATCH_SIZE = "geode-connector-batch-size";
  public static final String DEFAULT_BATCH_SIZE = "100";
  public static final String QUEUE_SIZE = "geode-connector-queue-size";
  public static final String DEFAULT_QUEUE_SIZE = "10000";
  public static final String LOAD_ENTIRE_REGION = "load-entire-region";
  public static final String DEFAULT_LOAD_ENTIRE_REGION = "false";
  public static final String
      CQS_TO_REGISTER_DOCUMENTATION =
      "Internally created and used parameter, for signalling a task to register CQs on Apache Geode";
  public static final String
      REGION_TO_TOPIC_BINDINGS_DOCUMENTATION =
      "A comma separated list of \"one region to many topics\" mappings.  Each mapping is surrounded by brackets.  For example \"[regionName:topicName], \"[anotherRegion: topicName, anotherTopic]\"";
  public static final String
      DURABLE_CLIENT_ID_PREFIX_DOCUMENTATION =
      "Prefix string for tasks to append to when registering as a durable client.  If empty string, will not register as a durable client";
  public static final String
      LOAD_ENTIRE_REGION_DOCUMENTATION =
      "Determines if we should queue up all entries that currently exist in a region.  This allows us to copy existing region data.  Will be replayed whenever a task needs to re-register a CQ";
  public static final String
      DURABLE_CLIENT_TIME_OUT_DOCUMENTATION =
      "How long in milliseconds to persist values in Geode's durable queue before the queue is invalidated";
  public static final String CQ_PREFIX_DOCUMENTATION = "Prefix string to identify Connector CQ's on a Geode server";
  public static final String BATCH_SIZE_DOCUMENTATION = "Maximum number of records to return on each poll";
  public static final String
      QUEUE_SIZE_DOCUMENTATION =
      "Maximum number of entries in the connector queue before backing up all Geode CQ listeners sharing the task queue ";
  public static final String SOURCE_GROUP = "Source-Configuration";
  public static final String CQS_TO_REGISTER_DISPLAY_NAME = "Continuous Queries (CQ) to register";
  public static final String REGION_TO_TOPIC_BINDINGS_DISPLAY_NAME = "Region to topic mapping";
  public static final String DURABLE_CLIENT_ID_PREFIX_DISPLAY_NAME = "Durable client ID";
  public static final String DURABLE_CLIENT_TIME_OUT_DISPLAY_NAME = "Durable Client timeout";
  public static final String CQ_PREFIX_DISPLAY_NAME = "CQ prefix";
  public static final String BATCH_SIZE_DISPLAY_NAME = "Batch size for CQs";
  public static final String QUEUE_SIZE_DISPLAY_NAME = "Queue size for CQs";
  public static final String LOAD_ENTIRE_REGION_DISPLAY_NAME = "Load entire region";
}
