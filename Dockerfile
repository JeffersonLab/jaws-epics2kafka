FROM gradle:6.6.1-jdk11 as builder

ARG CUSTOM_CRT_URL

USER root

RUN cd /tmp \
   && git clone https://github.com/JeffersonLab/epics2kafka-alarms \
   && cd epics2kafka-alarms \
    && if [ -z "$CUSTOM_CRT_URL" ] ; then echo "No custom cert needed"; else \
        wget -O /usr/local/share/ca-certificates/customcert.crt $CUSTOM_CRT_URL \
        && update-ca-certificates \
        && keytool -import -alias custom -file /usr/local/share/ca-certificates/customcert.crt -cacerts -storepass changeit -noprompt \
        && export OPTIONAL_CERT_ARG=-Djavax.net.ssl.trustStore=$JAVA_HOME/lib/security/cacerts \
        ; fi \
    && gradle build $OPTIONAL_CERT_ARG

FROM slominskir/epics2kafka:1.1.0

COPY --from=builder /tmp/epics2kafka-alarms/build/libs $KAFKA_CONNECT_PLUGINS_DIR/epics2kafka-alarms