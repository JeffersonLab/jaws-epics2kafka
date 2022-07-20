# jaws-epics2kafka [![CI](https://github.com/JeffersonLab/jaws-epics2kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/JeffersonLab/jaws-epics2kafka/actions/workflows/ci.yml) [![Docker](https://img.shields.io/docker/v/slominskir/jaws-epics2kafka?sort=semver&label=DockerHub)](https://hub.docker.com/r/slominskir/jaws-epics2kafka)
An extenstion to the [epics2kafka](https://github.com/JeffersonLab/epics2kafka) Kafka Connector that adds a [Transform](https://kafka.apache.org/documentation.html#connect_transforms) plugin to serialize messages in the format required by [JAWS](https://github.com/JeffersonLab/jaws).

---
- [Overview](https://github.com/JeffersonLab/jaws-epics2kafka#overview)
- [Quick Start with Compose](https://github.com/JeffersonLab/jaws-epics2kafka#quick-start-with-compose)
- [Install](https://github.com/JeffersonLab/jaws-epics2kafka#install)
- [Configure](https://github.com/JeffersonLab/jaws-epics2kafka#configure)  
- [Build](https://github.com/JeffersonLab/jaws-epics2kafka#build)
- [Release](https://github.com/JeffersonLab/jaws-epics2kafka#release)
---

## Overview
The following transformation is performed:

**Value**: [epics-monitor-event-value](https://github.com/JeffersonLab/epics2kafka/blob/2e30d5bcbadfc5e891999b18f170e4d8b243bbf2/src/main/java/org/jlab/kafka/connect/CASourceTask.java#L50-L61) -> [AlarmActivationUnion.avsc](https://github.com/JeffersonLab/jaws-libj/blob/main/src/main/avro/AlarmActivationUnion.avsc)

**Note**: epics2kafka must be configured to use the optional _outkey_ field to ensure the alarm name is used as the key and not the channel name, which is the default.  The [registrations2epics](https://github.com/JeffersonLab/registrations2epics) app handles this.

## Quick Start with Compose 
1. Grab project
```
git clone https://github.com/JeffersonLab/jaws-epics2kafka
cd jaws-epics2kafka
```
2. Launch Docker
```
docker compose up
```
3. Monitor the alarm-activations topic
```
docker exec -it jaws /scripts/client/list_activations.py --monitor
```
4. Trip an alarm
```
docker exec softioc caput channel1 1
```
5. Request invalid channel to verify error is provided
```
docker exec epics2kafka /scripts/set-monitored.sh -t alarm-activations -c invalid_channel -m va
```


See: [Docker Compose Strategy](https://gist.github.com/slominskir/a7da801e8259f5974c978f9c3091d52c)

## Install
Copy the jaws-epics2kafka.jar and it's core direct dependencies into a subdirectory of the Kafka plugins directory.  For example:
```
mkdir /opt/kafka/plugins/jaws-epics2kafka
cp jaws-epics2kafka*.jar /opt/kafka/plugins/jaws-epics2kafka
cp jaws-libj*.jar /opt/kafka/plugins/jaws-epics2kafka
cp kafka-common*.jar /opt/kafka/plugins/jaws-epics2kafka
```
**Note**: The `epics2kafka*.jar` should be in a separate plugins subdirectory from jaws-epics2kafka.  Since they share kafka-common*.jar it's likely safest to remove that jar from each plugin subdirectory and move it to kafka/libs.

You'll also need to ensure the plugin has access to it's platform dependencies: Confluent Kafka. Many are already in the kafka libs directory, but AVRO and Confluent AVRO/Registry related dependencies must be copied into Kafka libs (if you're not already using a Confluent distribution of Kafka).  The easiest way is to download the Confluent Community Edition and cherry pick a few jars out of it.  If you have Kafka installed at /opt/kafka and you want confluent at /opt/confluent then the steps would be:
```
wget http://packages.confluent.io/archive/7.1/confluent-community-7.1.1.zip
unzip -d /opt/confluent confluent-community-7.1.1.zip
cd /opt/confluent
mv confluent-7.1.1 7.1.1
cp /opt/confluent/7.1.1/share/java/confluent-common/common-utils-7.1.1.jar /opt/kafka/libs/
cp /opt/confluent/7.1.1/share/java/confluent-common/common-config-7.1.1.jar /opt/kafka/libs/
cp /opt/confluent/7.1.1/share/java/kafka-serde-tools/kafka-avro-serializer-7.1.1.jar /opt/kafka/libs/
cp /opt/confluent/7.1.1/share/java/kafka-serde-tools/kafka-connect-avro-data-7.1.1.jar /opt/kafka/libs
cp /opt/confluent/7.1.1/share/java/kafka-serde-tools/kafka-connect-avro-converter-7.1.1.jar /opt/kafka/libs
cp /opt/confluent/7.1.1/share/java/kafka-serde-tools/kafka-schema-serializer-7.1.1.jar /opt/kafka/libs
cp /opt/confluent/7.1.1/share/java/kafka-serde-tools/kafka-schema-registry-client-7.1.1.jar /opt/kafka/libs
cp /opt/confluent/7.1.1/share/java/kafka-serde-tools/avro-1.11.0.jar /opt/kafka/libs
cp /opt/confluent/7.1.1/share/java/kafka-serde-tools/guava-30.1.1-jre.jar /opt/kafka/libs
```

## Configure
The Connect configuration (JSON):
```
    "transforms": "alarmsValue",
    "transforms.alarmsValue.type": "org.jlab.jaws.EpicsToAlarm$Value
```

Set the environment variable `USE_NO_ACTIVATION=false` (defaults to true) to replace [NoActivation](https://github.com/JeffersonLab/jaws-libp/blob/627b07af785723a399400f5e79a007d7bd6839eb/src/jaws_libp/avro/schemas/AlarmActivationUnion.avsc#L103-L108) messages with tombstones (null) instead. 

## Build
This project is built with [Java 17](https://adoptium.net/) (compiled to Java 11 bytecode), and uses the [Gradle 7](https://gradle.org/) build tool to automatically download dependencies and build the project from source:

```
git clone https://github.com/JeffersonLab/jaws-epics2kafka
cd jaws-epics2kafka
gradlew installDist
```
**Note**: If you do not already have Gradle installed, it will be installed automatically by the wrapper script included in the source

**Note for JLab On-Site Users**: Jefferson Lab has an intercepting [proxy](https://gist.github.com/slominskir/92c25a033db93a90184a5994e71d0b78)

**See**: [Docker Development Quick Reference](https://gist.github.com/slominskir/a7da801e8259f5974c978f9c3091d52c#development-quick-reference)

## Release
1. Bump the version number in build.gradle and commit and push to GitHub (using [Semantic Versioning](https://semver.org/)).
2. Create a new release on the GitHub Releases page corresponding to the same version in the build.gradle.   The release should enumerate changes and link issues.   A zip artifact generated from the gradle distZip target can be attached to the release to facilitate easy install by users.
3. Build and publish a new Docker image [from the GitHub tag](https://gist.github.com/slominskir/a7da801e8259f5974c978f9c3091d52c#8-build-an-image-based-of-github-tag).
4. Bump and commit quick start [image version](https://github.com/JeffersonLab/jaws-epics2kafka/blob/main/docker-compose.override.yml)
