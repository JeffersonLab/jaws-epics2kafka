# kafka-transform-epics
Kafka Connect Transform from [epics2kafka](https://github.com/JeffersonLab/epics2kafka) to the [kafka-alarm-system](https://github.com/JeffersonLab/kafka-alarm-system).

The jar file is available on [Bintray](https://dl.bintray.com/slominskir/maven/org/jlab/kafka/connect/transform/kafka-transform-epics/) if you like to use maven artifiacts in your build.

## Transformations

### Key
Alarm Name -> [active-alarms-key.avsc](https://github.com/JeffersonLab/kafka-alarm-system/blob/master/schemas/active-alarms-key.avsc)

### Value
[epics-monitor-event-value](https://github.com/JeffersonLab/epics2kafka/blob/master/src/main/java/org/jlab/kafka/connect/CASourceTask.java#L42-L55) -> [active-alarms-value.avsc](https://github.com/JeffersonLab/kafka-alarm-system/blob/master/schemas/active-alarms-value.avsc)

## Build
```
gradlew build
```
