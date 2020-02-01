## What is geode-kafka-connector

Kafka provides an integration point through Source and Sink Connectors.  The GeodeKafkaSource allows Geode to be a data source for Kafka
The GeodeKafkaSink allows Geode to consume data off of topics and store data from Kafka.

### How to install the geode-kafka-connector
---
#### Prequisite
* Kafka is installed and is up and running.  See the Kafka quickstart for more info: [Kafka Quickstart](https://kafka.apache.org/quickstart)
* A Geode Cluster with at least one locator and one server and regions to source from and sink to.
* Topics created in Kafka to source from and sink to.
---
Installation of the connector is similar in process to other Kafka Connectors.  For now, we will follow the guide for [Manual Installation](https://docs.confluent.io/current/connect/managing/install.html#install-connector-manually).

In summary, we will use the standalone worker for this example.
* Explode a zip file or build into a known (and Kafka accessible) location
* Modify the connect-standalone.properties and point to where the connector is installed.
```
plugin.path=/Users/jhuynh/Pivotal/geode-kafka-connector/build/libs/
```
* Create and modify connect-geode-source.properties file, for example
```
name=geode-kafka-sink
connector.class=GeodeKafkaSink
tasks.max=1
topicToRegions=[someTopicToSinkFrom:someRegionToConsume]
locators=localHost[10334]
```
* Create and modify connect-geode-sink.properties files
```
name=geode-kafka-source
connector.class=GeodeKafkaSource
tasks.max=1
regionToTopics=[someRegionToSourceFrom:someTopicToConsume]
locators=localHost[10334]
```

* Run
bin/connect-standalone.sh config/connect-standalone.properties config/connect-geode-source.properties config/connect-geode-sink.properties


---
#### GeodeKafkaSink Properties
| Property | Required | Description| Default |
|---|---|---|---|
| locators | no, but...| A comma separated string of locators that configure which locators to connect to | localhost[10334] |
|topicToRegions| yes| A comma separated list of "one topic to many regions" bindings.  Each binding is surrounded by brackets. For example "[topicName:regionName], [anotherTopic: regionName, anotherRegion]" | None.  This is required to be set in the source connector properties
|security-client-auth-init| no | Point to class that implements the [AuthInitialize Interface](https://gemfire.docs.pivotal.io/99/geode/managing/security/implementing_authentication.html)
|nullValuesMeanRemove | no | If set to true, when topics send a SinkRecord with a null value, we will convert to an operation similar to region.remove instead of putting a null value into the region | true |

* The topicToRegions property allows us to create mappings between topics  and regions.  A single one-to-one mapping would look similar to "[topic:region]" A one-to-many mapping can be made by comma separating the regions, for example "[topic:region1,region2]"  This is equivalent to both regions being consumers of the topic.

#### GeodeKafkaSource Properties
| Property | Required| Description| Default |
|---|---|---|---|
| locators | no, but...| A comma separated string of locators that configure which locators to connect to | localhost[10334] |
|regionToTopics| yes | A comma separated list of "one region to many topics" mappings.  Each mapping is surrounded by brackets.  For example "[regionName:topicName], "[anotherRegion: topicName, anotherTopic]" | None.  This is required to be set in the source connector properties|
|security-client-auth-init| no | Point to class that implements the [AuthInitialize Interface](https://gemfire.docs.pivotal.io/99/geode/managing/security/implementing_authentication.html)
|geodeConnectorBatchSize| no | Maximum number of records to return on each poll| 100 |
|geodeConnectorQueueSize| no | Maximum number of entries in the connector queue before backing up all Geode cq listeners sharing the task queue | 10000 |
| loadEntireRegion| no| Determines if we should queue up all entries that currently exist in the region.  This allows us to copy existing region data.  Will be replayed whenever a task needs to re-register a cq| true |
|durableClientIdPrefix| no | Prefix string for tasks to append to when registering as a durable client.  If empty string, will not register as a durable client | "" |
| durableClientTimeout| no | How long in milliseconds to persist values in Geode's durable queue before the queue is invalidated| 60000 |
| cqPrefix| no| Prefix string to identify Connector cq's on a Geode server |cqForGeodeKafka |

* The regionToTopics property allows us to create mappings between regions and topics.  A single one-to-one mapping would look similar to "[region:topic]" A one-to-many mapping can be made by comma separating the topics, for example "[region:topic1,topic2]"  This is equivalent to the region be a producer for both topics 

---

* Consider modifying Kafka Properties like tasks.max in the source and sink parameters.

### Possible Upcoming Featured:
* Formatters - Possibly a JSON to and from PDX formatter
* Security - security settings for Geode
* Dynamic Region creation - Dynamically create regions when topics are created (filter what names to look for and what types of regions to create)
* Allow a single worker to connect to multiple Geode Clusters?
