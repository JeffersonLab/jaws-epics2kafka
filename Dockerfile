ARG BUILD_IMAGE=gradle:7.4-jdk17-alpine
ARG RUN_IMAGE=jeffersonlab/epics2kafka:2.0.0
ARG CUSTOM_CRT_URL=http://pki.jlab.org/JLabCA.crt

################## Stage 0
FROM ${BUILD_IMAGE} AS builder
ARG CUSTOM_CRT_URL
USER root
WORKDIR /
RUN if [ -z "${CUSTOM_CRT_URL}" ] ; then echo "No custom cert needed"; else \
       wget -O /usr/local/share/ca-certificates/customcert.crt $CUSTOM_CRT_URL \
       && update-ca-certificates \
       && keytool -import -alias custom -file /usr/local/share/ca-certificates/customcert.crt -cacerts -storepass changeit -noprompt \
       && export OPTIONAL_CERT_ARG=--cert=/etc/ssl/certs/ca-certificates.crt \
    ; fi
COPY . /app
RUN cd /app && gradle build -x test --no-watch-fs $OPTIONAL_CERT_ARG

################## Stage 1
FROM ${RUN_IMAGE} AS runner
ARG RUN_USER=1001
ARG CUSTOM_CRT_URL
USER root
COPY --from=builder /app/build/install $KAFKA_CONNECT_PLUGINS_DIR
RUN if [ -z "${CUSTOM_CRT_URL}" ] ; then echo "No custom cert needed"; else \
           curl -o /usr/local/share/ca-certificates/customcert.crt $CUSTOM_CRT_URL \
           && update-ca-certificates \
           && keytool -import -alias custom -file /usr/local/share/ca-certificates/customcert.crt -cacerts -storepass changeit -noprompt \
           && export OPTIONAL_CERT_ARG=--cert=/etc/ssl/certs/ca-certificates.crt \
        ; fi \
    && cd $KAFKA_HOME/libs  \
    && curl -O https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.2/avro-1.11.2.jar \
    && curl -O https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/7.4.0/kafka-schema-registry-client-7.4.0.jar \
    && curl -O https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/7.4.0/kafka-schema-serializer-7.4.0.jar \
    && curl -O https://packages.confluent.io/maven/io/confluent/kafka-schema-converter/7.4.0/kafka-schema-converter-7.4.0.jar \
    && curl -O https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/7.4.0/kafka-avro-serializer-7.4.0.jar \
    && curl -O https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/7.4.0/kafka-connect-avro-converter-7.4.0.jar \
    && curl -O https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-data/7.4.0/kafka-connect-avro-data-7.4.0.jar \
    && curl -O https://packages.confluent.io/maven/io/confluent/common-utils/7.4.0/common-utils-7.4.0.jar \
    && curl -O https://packages.confluent.io/maven/io/confluent/common-config/7.4.0/common-config-7.4.0.jar \
    && curl -O https://repo1.maven.org/maven2/com/google/guava/guava/30.1.1-jre/guava-30.1.1-jre.jar \
    && curl -O https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar
USER ${RUN_USER}
