# kafka-transform-epics [![Build Status](https://travis-ci.com/JeffersonLab/kafka-transform-epics.svg?branch=master)](https://travis-ci.com/JeffersonLab/kafka-transform-epics) [ ![Download](https://api.bintray.com/packages/slominskir/maven/kafka-transform-epics/images/download.svg?version=0.15.0) ](https://bintray.com/slominskir/maven/kafka-transform-epics/0.15.0/link)
Kafka Connect [Transform](https://kafka.apache.org/documentation.html#connect_transforms) message serialization format from [epics2kafka](https://github.com/JeffersonLab/epics2kafka) to the [kafka-alarm-system](https://github.com/JeffersonLab/kafka-alarm-system).

The jar file is available on [Bintray](https://dl.bintray.com/slominskir/maven/org/jlab/kafka/connect/transform/kafka-transform-epics/) if you like to use maven artifiacts in your build.

## Transformations

### Key
Alarm Name -> [active-alarms-key.avsc](https://github.com/JeffersonLab/kafka-alarm-system/blob/master/schemas/active-alarms-key.avsc)

**Note**: epics2kafka must be configured to use the optional _outkey_ field to ensure the alarm name is used as the key and not the channel name, which is the default.  The [registrations2epics](https://github.com/JeffersonLab/registrations2epics) app handles this.

### Value
[epics-monitor-event-value](https://github.com/JeffersonLab/epics2kafka/blob/master/src/main/java/org/jlab/kafka/connect/CASourceTask.java#L42-L54) -> [active-alarms-value.avsc](https://github.com/JeffersonLab/kafka-alarm-system/blob/master/schemas/active-alarms-value.avsc)

## Build
```
gradlew build
```
## Deploy
Copy the kafka-transform-epics.jar file into a subdirectory of the Kafka plugins directory.  For example:
```
mkdir /opt/kafka/plugins/alarm-transform
cp kafka-transform-epics.jar /opt/kafka/plugins/alarm-transform
```
## Configure
The Connect configuration (JSON):
```
    "transforms": "alarmsKey,alarmsValue",
    "transforms.alarmsKey.type": "org.jlab.kafka.connect.transforms.EpicsToAlarm$Key",
    "transforms.alarmsValue.type": "org.jlab.kafka.connect.transforms.EpicsToAlarm$Value
```
