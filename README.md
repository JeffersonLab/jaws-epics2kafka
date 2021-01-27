# epics2kafka-alarms [![Java CI with Gradle](https://github.com/JeffersonLab/epics2kafka-alarms/workflows/Java%20CI%20with%20Gradle/badge.svg)](https://github.com/JeffersonLab/epics2kafka-alarms/actions?query=workflow%3A%22Java+CI+with+Gradle%22) [![Download](https://api.bintray.com/packages/slominskir/maven/epics2kafka-alarms/images/download.svg) ](https://bintray.com/slominskir/maven/epics2kafka-alarms) [![Docker Image Version (latest semver)](https://img.shields.io/docker/v/slominskir/epics2kafka-alarms?sort=semver)   ](https://hub.docker.com/r/slominskir/epics2kafka-alarms)
An extenstion to the [epics2kafka](https://github.com/JeffersonLab/epics2kafka) Kafka Connector that adds a [Transform](https://kafka.apache.org/documentation.html#connect_transforms) plugin to serialize messages in the format required by the [kafka-alarm-system](https://github.com/JeffersonLab/kafka-alarm-system).

---
- [Overview](https://github.com/JeffersonLab/kafka-alarm-system#overview)
- [Quick Start with Compose](https://github.com/JeffersonLab/kafka-alarm-system#quick-start-with-compose)
- [Build](https://github.com/JeffersonLab/kafka-alarm-system#build)
- [Deploy](https://github.com/JeffersonLab/kafka-alarm-system#deploy)
- [Configure](https://github.com/JeffersonLab/kafka-alarm-system#configure)
- [Docker](https://github.com/JeffersonLab/kafka-alarm-system#docker)
---

## Overview
The following transformations are performed:

**Key**: Alarm Name -> [active-alarms-key.avsc](https://github.com/JeffersonLab/kafka-alarm-system/blob/master/schemas/active-alarms-key.avsc)

**Value**: [epics-monitor-event-value](https://github.com/JeffersonLab/epics2kafka/blob/master/src/main/java/org/jlab/kafka/connect/CASourceTask.java#L42-L54) -> [active-alarms-value.avsc](https://github.com/JeffersonLab/kafka-alarm-system/blob/master/schemas/active-alarms-value.avsc)

**Note**: epics2kafka must be configured to use the optional _outkey_ field to ensure the alarm name is used as the key and not the channel name, which is the default.  The [registrations2epics](https://github.com/JeffersonLab/registrations2epics) app handles this.

## Quick Start with Compose 
1. Grab project
```
git clone https://github.com/JeffersonLab/kafka-transform-epics
cd kafka-transform-epics
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
docker exec -it console /scripts/list-active.py
```

**Note**: When developing the app you can mount the build artifact into the container by substituting the `docker-compose up` command with:
```
docker-compose -f docker-compose.yml -f docker-compose-dev.yml up
```

## Build
This [Java 11](https://adoptopenjdk.net/) project uses the [Gradle 6](https://gradle.org/) build tool to automatically download dependencies and build the project from source:

```
git clone https://github.com/JeffersonLab/kafka-transform-epics
cd kafka-transform-epics
gradlew build
```
**Note**: If you do not already have Gradle installed, it will be installed automatically by the wrapper script included in the source

**Note**: Jefferson Lab has an intercepting [proxy](https://gist.github.com/slominskir/92c25a033db93a90184a5994e71d0b78)
## Deploy
Copy the epics2kafka-alarms.jar file into a subdirectory of the Kafka plugins directory.  For example:
```
mkdir /opt/kafka/plugins/epics2kafka-alarms
cp epics2kafka-alarms.jar /opt/kafka/plugins/epics2kafka-alarms
```
**Note**: The jar file is available on [Bintray](https://dl.bintray.com/slominskir/maven/org/jlab/kafka/connect/transform/epics2kafka-alarms/).
## Configure
The Connect configuration (JSON):
```
    "transforms": "alarmsKey,alarmsValue",
    "transforms.alarmsKey.type": "org.jlab.kafka.connect.transforms.EpicsToAlarm$Key",
    "transforms.alarmsValue.type": "org.jlab.kafka.connect.transforms.EpicsToAlarm$Value
```
## Docker
```
docker pull slominskir/epics2kafka-alarms
```
Image hosted on [DockerHub](https://hub.docker.com/r/slominskir/epics2kafka-alarms)
