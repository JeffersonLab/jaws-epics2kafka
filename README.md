# epics2kafka-alarms [![Java CI with Gradle](https://github.com/JeffersonLab/epics2kafka-alarms/workflows/Java%20CI%20with%20Gradle/badge.svg)](https://github.com/JeffersonLab/epics2kafka-alarms/actions?query=workflow%3A%22Java+CI+with+Gradle%22) [![Docker](https://img.shields.io/docker/v/slominskir/epics2kafka-alarms?sort=semver&label=DockerHub)](https://hub.docker.com/r/slominskir/epics2kafka-alarms)
An extenstion to the [epics2kafka](https://github.com/JeffersonLab/epics2kafka) Kafka Connector that adds a [Transform](https://kafka.apache.org/documentation.html#connect_transforms) plugin to serialize messages in the format required by [JAWS](https://github.com/JeffersonLab/jaws).

---
- [Overview](https://github.com/JeffersonLab/epics2kafka-alarms#overview)
- [Quick Start with Compose](https://github.com/JeffersonLab/epics2kafka-alarms#quick-start-with-compose)
- [Build](https://github.com/JeffersonLab/epics2kafka-alarms#build)
- [Deploy](https://github.com/JeffersonLab/epics2kafka-alarms#deploy)
- [Configure](https://github.com/JeffersonLab/epics2kafka-alarms#configure)
- [Docker](https://github.com/JeffersonLab/epics2kafka-alarms#docker)
---

## Overview
The following transformation is performed:

**Value**: [epics-monitor-event-value](https://github.com/JeffersonLab/epics2kafka/blob/master/src/main/java/org/jlab/kafka/connect/CASourceTask.java#L42-L54) -> [active-alarms-value.avsc](https://github.com/JeffersonLab/jaws-libj/blob/main/src/main/avro/active-alarms-value.avsc)

**Note**: epics2kafka must be configured to use the optional _outkey_ field to ensure the alarm name is used as the key and not the channel name, which is the default.  The [registrations2epics](https://github.com/JeffersonLab/registrations2epics) app handles this.

## Quick Start with Compose 
1. Grab project
```
git clone https://github.com/JeffersonLab/epics2kafka-alarms
cd epics2kafka-alarms
```
2. Launch Docker
```
docker-compose up
```
3. Trip an alarm
```
docker exec softioc caput channel1 1
```
4. Verify that the active-alarms topic received a properly formatted message 
```
docker exec -it jaws /scripts/client/list-active.py
```
## Build
This [Java 11](https://adoptopenjdk.net/) project uses the [Gradle 6](https://gradle.org/) build tool to automatically download dependencies and build the project from source:

```
git clone https://github.com/JeffersonLab/epics2kafka-alarms
cd epics2kafka-alarms
gradlew build
```
**Note**: If you do not already have Gradle installed, it will be installed automatically by the wrapper script included in the source

**Note**: Jefferson Lab has an intercepting [proxy](https://gist.github.com/slominskir/92c25a033db93a90184a5994e71d0b78)

**Note**: When developing the app you can mount the build artifact into the container by substituting the `docker-compose up` command with:
```
docker-compose -f docker-compose.yml -f docker-compose-dev.yml up
```
## Deploy
Copy the epics2kafka-alarms.jar file into a subdirectory of the Kafka plugins directory.  For example:
```
mkdir /opt/kafka/plugins/epics2kafka-alarms
cp epics2kafka-alarms.jar /opt/kafka/plugins/epics2kafka-alarms
```
**Note**: You'll also need to ensure the plugin has access to it's dependencies.   Specifically you'll need to copy the _jaws-libj.jar_ file into the _plugins/epics2kafka-alarms_ directory as well.   You might even need to setup a symbolic link inside the same directory (perhaps named "deps") pointing to the _/usr/share/java/kafka-serdes-tools_ directory or equivalent such that Confluent AVRO and Schema Registry depdnences are resolved (depends on what is part of the core Kafka install).
## Configure
The Connect configuration (JSON):
```
    "transforms": "alarmsValue",
    "transforms.alarmsValue.type": "org.jlab.jaws.EpicsToAlarm$Value
```
## Docker
A Docker container with both epics2kafka and the epics2kafka-alarms transform installed:
```
docker pull slominskir/epics2kafka-alarms
```
Image hosted on [DockerHub](https://hub.docker.com/r/slominskir/epics2kafka-alarms)
