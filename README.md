# jtransforms
Kafka Connect Transforms Library for Jefferson Lab

At this time there is only a single transform: EpicsToAlarm, which is used to transform messages produced by [epics2kafka](https://github.com/JeffersonLab/epics2kafka) to a format that matches the [kafka-alarm-system](https://github.com/JeffersonLab/kafka-alarm-system).

The jar file is available on [Bintray](https://dl.bintray.com/slominskir/maven/org/jlab/kafka/connect/transform/jtransforms/) if you like to use maven artifiacts in your build.
