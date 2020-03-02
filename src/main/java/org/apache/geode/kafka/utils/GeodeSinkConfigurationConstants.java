package org.apache.geode.kafka.utils;

public class GeodeSinkConfigurationConstants {
  /**
   * SINK SPECIFIC CONFIGURATION
   */
  public static final String TOPIC_TO_REGION_BINDINGS = "topic-to-regions";
  public static final String DEFAULT_TOPIC_TO_REGION_BINDING = "[gkcTopic:gkcRegion]";
  public static final String NULL_VALUES_MEAN_REMOVE = "null-values-mean-remove";
  public static final String DEFAULT_NULL_VALUES_MEAN_REMOVE = "true";
  public static final String
      NULL_VALUES_MEAN_REMOVE_DOCUMENTATION =
      "If set to true, when topics send a SinkRecord with a null value, we will convert to an operation similar to region.remove instead of putting a null value into the region";
  public static final String
      TOPIC_TO_REGION_BINDINGS_DOCUMENTATION =
      "A comma separated list of \"one topic to many regions\" bindings.  Each binding is surrounded by brackets. For example \"[topicName:regionName], [anotherTopic: regionName, anotherRegion]";
  public static final String SINK_GROUP = "Sink-Configurations";
  public final static String TOPIC_TO_REGION_BINDINGS_DISPLAY_NAME = "Topic to region mapping";
  public final static String NULL_VALUES_MEAN_REMOVE_DISPLAY_NAME = "Null values behavior";

}
