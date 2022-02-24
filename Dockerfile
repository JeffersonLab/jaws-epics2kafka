FROM gradle:7.4-jdk17 as builder

ARG CUSTOM_CRT_URL

USER root

RUN cd /tmp \
   && git clone https://github.com/JeffersonLab/jaws-epics2kafka \
   && cd jaws-epics2kafka \
    && if [ -z "$CUSTOM_CRT_URL" ] ; then echo "No custom cert needed"; else \
        wget -O /usr/local/share/ca-certificates/customcert.crt $CUSTOM_CRT_URL \
        && update-ca-certificates \
        && keytool -import -alias custom -file /usr/local/share/ca-certificates/customcert.crt -cacerts -storepass changeit -noprompt \
        && export OPTIONAL_CERT_ARG=-Djavax.net.ssl.trustStore=$JAVA_HOME/lib/security/cacerts \
        ; fi \
    && gradle installDist $OPTIONAL_CERT_ARG

FROM slominskir/epics2kafka:1.3.0

COPY --from=builder /tmp/jaws-epics2kafka/build/install $KAFKA_CONNECT_PLUGINS_DIR/jaws-epics2kafka