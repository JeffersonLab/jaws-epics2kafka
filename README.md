# jtransforms
Kafka Connect Transforms Library for Jefferson Lab

At this time there is only a single transform: EpicsToAlarm, which is used to transform messages produced by [epics2kafka](https://github.com/JeffersonLab/epics2kafka) to a format that matches the [kafka-alarm-system](https://github.com/JeffersonLab/kafka-alarm-system).

[epics-monitor-event](https://github.com/JeffersonLab/epics2kafka/blob/master/src/main/java/org/jlab/kafka/connect/CASourceTask.java#L42-L52) -> [active-alarms-value.avsc](https://github.com/JeffersonLab/kafka-alarm-system/blob/master/schemas/active-alarms-value.avsc)

The jar file is available on [Bintray](https://dl.bintray.com/slominskir/maven/org/jlab/kafka/connect/transform/jtransforms/) if you like to use maven artifiacts in your build.
