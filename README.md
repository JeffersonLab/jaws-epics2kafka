# jaws-epics2kafka [![Java CI with Gradle](https://github.com/JeffersonLab/jaws-epics2kafka/actions/workflows/gradle.yml/badge.svg)](https://github.com/JeffersonLab/jaws-epics2kafka/actions/workflows/gradle.yml) [![Docker](https://img.shields.io/docker/v/slominskir/jaws-epics2kafka?sort=semver&label=DockerHub)](https://hub.docker.com/r/slominskir/jaws-epics2kafka)
An extenstion to the [epics2kafka](https://github.com/JeffersonLab/epics2kafka) Kafka Connector that adds a [Transform](https://kafka.apache.org/documentation.html#connect_transforms) plugin to serialize messages in the format required by [JAWS](https://github.com/JeffersonLab/jaws).

---
- [Overview](https://github.com/JeffersonLab/jaws-epics2kafka#overview)
- [Quick Start with Compose](https://github.com/JeffersonLab/jaws-epics2kafka#quick-start-with-compose)
- [Install](https://github.com/JeffersonLab/jaws-epics2kafka#install)
- [Configure](https://github.com/JeffersonLab/jaws-epics2kafka#configure)  
- [Build](https://github.com/JeffersonLab/jaws-epics2kafka#build)
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
Copy the jaws-epics2kafka.jar file into a subdirectory of the Kafka plugins directory.  For example:
```
mkdir /opt/kafka/plugins/jaws-epics2kafka
cp jaws-epics2kafka.jar /opt/kafka/plugins/jaws-epics2kafka
```
**Note**: You'll also need to ensure the plugin has access to it's dependencies by either copying them into the kafka lib directory OR the plugin subdirectory `jaws-epics2kafka`.  This is a little tricky since many of the indirect dependencies are already installed in kafka lib (log4j, slf4j, jackson).   You can simply copy the contents of release zip lib directory (created by `gradle installDist`) into the plugin subdirectory, as the overlap doesn't appear to be a problem.  Else you can cherry pick the following from it if you're trying to minimize duplicate jars:
- jaws-libj
- kafka-common
- avro
- kafka-connect-avro-data
- kafka-schema-registry-client
- kafka-schema-serializer
- kafka-clients
- kafka-avro-serializer
- common-utils

## Configure
The Connect configuration (JSON):
```
    "transforms": "alarmsValue",
    "transforms.alarmsValue.type": "org.jlab.jaws.EpicsToAlarm$Value
```

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
