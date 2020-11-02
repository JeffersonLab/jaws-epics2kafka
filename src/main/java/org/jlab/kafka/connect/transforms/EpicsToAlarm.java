package org.jlab.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.jlab.kafka.connect.transforms.util.SeverityEnum;
import org.jlab.kafka.connect.transforms.util.StatusEnum;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class EpicsToAlarm<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC =
            "Transform epics2kafka messages to kafka-alarm-system format";

    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    private static final String PURPOSE = "transform epics2kafka messages to kafka-alarm-system format";

    final Schema sevrSchema = SchemaBuilder
            .string()
            .doc("Alarming state (EPICS .SEVR field)")
            .parameter("io.confluent.connect.avro.enum.doc.SevrEnum", "Enumeration of possible EPICS .SEVR values")
            .parameter("io.confluent.connect.avro.Enum", "org.jlab.kafka.alarms.SevrEnum")
            .parameter("io.confluent.connect.avro.Enum.1", "NO_ALARM")
            .parameter("io.confluent.connect.avro.Enum.2", "MINOR")
            .parameter("io.confluent.connect.avro.Enum.3", "MAJOR")
            .parameter("io.confluent.connect.avro.Enum.4", "INVALID")
            .build();

    final Schema statSchema = SchemaBuilder
            .string()
            .doc("Alarming status (EPICS .STAT field)")
            .parameter("io.confluent.connect.avro.enum.doc.StatEnum", "Enumeration of possible EPICS .STAT values")
            .parameter("io.confluent.connect.avro.Enum", "org.jlab.kafka.alarms.StatEnum")
            .parameter("io.confluent.connect.avro.Enum.1", "NO_ALARM")
            .parameter("io.confluent.connect.avro.Enum.2", "READ")
            .parameter("io.confluent.connect.avro.Enum.3", "WRITE")
            .parameter("io.confluent.connect.avro.Enum.4", "HIHI")
            .parameter("io.confluent.connect.avro.Enum.5", "HIGH")
            .parameter("io.confluent.connect.avro.Enum.6", "LOLO")
            .parameter("io.confluent.connect.avro.Enum.7", "LOW")
            .parameter("io.confluent.connect.avro.Enum.8", "STATE")
            .parameter("io.confluent.connect.avro.Enum.9", "COS")
            .parameter("io.confluent.connect.avro.Enum.10", "COMM")
            .parameter("io.confluent.connect.avro.Enum.11", "TIMEOUT")
            .parameter("io.confluent.connect.avro.Enum.12", "HW_LIMIT")
            .parameter("io.confluent.connect.avro.Enum.13", "CALC")
            .parameter("io.confluent.connect.avro.Enum.14", "SCAN")
            .parameter("io.confluent.connect.avro.Enum.15", "LINK")
            .parameter("io.confluent.connect.avro.Enum.16", "SOFT")
            .parameter("io.confluent.connect.avro.Enum.17", "BAD_SUB")
            .parameter("io.confluent.connect.avro.Enum.18", "UDF")
            .parameter("io.confluent.connect.avro.Enum.19", "DISABLE")
            .parameter("io.confluent.connect.avro.Enum.20", "SIMM")
            .parameter("io.confluent.connect.avro.Enum.21", "READ_ACCESS")
            .parameter("io.confluent.connect.avro.Enum.22", "WRITE_ACCESS")
            .build();

    final Schema ackEnumSchema = SchemaBuilder
            .string()
            .doc("Indicates whether this alarm has been explicitly acknowledged - useful for latching alarms which can only be cleared after acknowledgement")
            .parameter("io.confluent.connect.avro.enum.doc.EPICSAcknowledgementEnum", "Enumeration of possible EPICS acknowledgement states")
            .parameter("io.confluent.connect.avro.Enum", "org.jlab.kafka.alarms.EPICSAcknowledgementEnum")
            .parameter("io.confluent.connect.avro.Enum.1", "NO_ACK")
            .parameter("io.confluent.connect.avro.Enum.2", "MINOR_ACK")
            .parameter("io.confluent.connect.avro.Enum.3", "MAJOR_ACK")
            .build();

    final Schema typeSchema = SchemaBuilder
            .string()
            .doc("The type of message included in the value - required as part of the key to ensure compaction keeps the latest message of each type")
            .parameter("io.confluent.connect.avro.enum.doc.ActiveMessageType", "Enumeration of possible message types")
            .parameter("io.confluent.connect.avro.Enum", "org.jlab.kafka.alarms.ActiveMessageType")
            .parameter("io.confluent.connect.avro.Enum.1", "Alarming")
            .parameter("io.confluent.connect.avro.Enum.2", "Ack")
            .parameter("io.confluent.connect.avro.Enum.3", "AlarmingEPICS")
            .parameter("io.confluent.connect.avro.Enum.4", "AckEPICS")
            .build();

    final Schema alarmingSchema = SchemaBuilder
            .struct()
            .name("org.jlab.kafka.alarms.Alarming")
            .doc("Alarming state for a basic alarm")
            .field("alarming", SchemaBuilder.bool().doc("true if the alarm is alarming, false otherwise").build())
            .build();

    final Schema ackSchema = SchemaBuilder
            .struct()
            .name("org.jlab.kafka.alarms.Ack")
            .doc("A basic acknowledgment message")
            .field("acknowledged", SchemaBuilder.bool().doc("true if the alarm is acknowledged, false otherwise").build())
            .build();

    final Schema alarmingEPICSSchema = SchemaBuilder
            .struct()
            .name("org.jlab.kafka.alarms.AlarmingEPICS")
            .doc("EPICS alarming state")
            .field("sevr", sevrSchema)
            .field("stat", statSchema)
            .build();

    final Schema ackEPICSSchema = SchemaBuilder
            .struct()
            .name("org.jlab.kafka.alarms.AckEPICS")
            .doc("EPICS acknowledgement state")
            .field("ack", ackEnumSchema)
            .build();

    final Schema msgSchema = SchemaBuilder
            .struct()
            .name("io.confluent.connect.avro.Union")
            .doc("Two types of messages are allowed: Alarming and Acknowledgement; There can be multiple flavors of each type for different alarm producers; modeled as a nested union to avoid complications of union at root of schema.")
            .field("Alarming", alarmingSchema)
            .optional()
            .field("Ack", ackSchema)
            .optional()
            .field("AlarmingEPICS", alarmingEPICSSchema)
            .optional()
            .field("AckEPICS", ackEPICSSchema)
            .optional()
            .build();

    final Schema updatedValueSchema = SchemaBuilder
            .struct()
            .name("org.jlab.kafka.alarms.ActiveAlarmValue")
            .doc("Alarming and Acknowledgements state")
            .version(1)
            .field("msg", msgSchema)
            .build();

    final Schema updatedKeySchema = SchemaBuilder
            .struct()
            .name("org.jlab.kafka.alarms.ActiveAlarmKey")
            .doc("Active alarms state (alarming or acknowledgment)")
            .version(1)
            .field("name", SchemaBuilder.string().doc("The unique name of the alarm").build())
            .field("type", typeSchema)
            .build();

    final ConnectHeaders headers = new ConnectHeaders();

    {
        String user = System.getProperty("user.name");
        String hostname = "localhost";

        if(user == null) {
            user = "";
        }

        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            System.err.println("Unable to determine hostname");
            e.printStackTrace(); // We could do better...
        }

        headers.addString("user", user);
        headers.addString("host", hostname);
        headers.addString("producer", "epics2alarms");
    }

    /**
     * Apply transformation to the {@code record} and return another record object (which may be {@code record} itself) or {@code null},
     * corresponding to a map or filter operation respectively.
     * <p>
     * The implementation must be thread-safe.
     *
     * @param record
     */
    @Override
    public R apply(R record) {
        System.err.println("apply!");
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        System.err.println("applySchemaless!");
        final Map<String, Object> map = requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedMap = doUpdate(map);

        return newRecord(record, null, updatedMap);
    }

    private R applyWithSchema(R record) {
        System.err.println("applyWithSchema!");
        final Struct struct = requireStruct(operatingValue(record), PURPOSE);

        //Schema schema = operatingSchema(record);

        final Struct updatedStruct = doUpdate(struct, updatedValueSchema);

        return newRecord(record, updatedValueSchema, updatedStruct);
    }

    /**
     * Configuration specification for this transformation.
     **/
    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    /**
     * Signal that this transformation instance will no longer will be used.
     **/
    @Override
    public void close() {
    }

    /**
     * Configure this class with the given key-value pairs
     *
     * @param configs
     */
    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    }

    protected abstract Map<String, Object> doUpdate(Map<String, Object> original);

    protected abstract Struct doUpdate(Struct original, Schema updatedSchema);

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends EpicsToAlarm<R> {

        @Override
        protected Map<String, Object> doUpdate(Map<String, Object> original) {
            System.err.println("map doUpdate (key)!");

            Map<String, Object> updated = new HashMap<>();

            for(String key: original.keySet()) {
                System.err.println("found key: " + key);
            }

            String name = (String)original.values().iterator().next();

            System.err.println("name: " + name);

            updated.put("name", name);
            updated.put("type", "AlarmingEPICS");

            return updated;
        }

        @Override
        protected Struct doUpdate(Struct original, Schema updatedSchema) {
            System.err.println("struct doUpdate (key)!");

            Struct updated = new Struct(updatedSchema);

            String name = original.toString();

            System.err.println("name: " + name);

            updated.put("name", name);
            updated.put("type", "AlarmingEPICS");

            return updated;
        }

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp(), headers);
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends EpicsToAlarm<R> {

        @Override
        protected Map<String, Object> doUpdate(Map<String, Object> original) {
            System.err.println("map doUpdate (value)!");

            Map<String, Object> updated = new HashMap<>();
            Map<String, Object> msg = new HashMap<>();
            updated.put("msg", msg);

            byte severity = (byte)original.get("severity");
            byte status = (byte)original.get("status");

            System.err.println("severity: " + severity);
            System.err.println("status: " + status);

            String sevrStr = SeverityEnum.fromOrdinal(severity).name();
            String statStr = StatusEnum.fromOrdinal(status).name();

            msg.put("sevr", sevrStr);
            msg.put("stat", statStr);

            return updated;
        }

        @Override
        protected Struct doUpdate(Struct original, Schema updatedSchema) {
            System.err.println("struct doUpdate (value)!");

            Struct updated = new Struct(updatedSchema);
            Struct msg = new Struct(alarmingEPICSSchema);
            updated.put("msg", msg);

            byte severity = original.getInt8("severity");
            byte status = original.getInt8("status");

            System.err.println("severity: " + severity);
            System.err.println("status: " + status);

            String sevrStr = SeverityEnum.fromOrdinal(severity).name();
            String statStr = StatusEnum.fromOrdinal(status).name();

            msg.put("sevr", sevrStr);
            msg.put("stat", statStr);

            return updated;
        }

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp(), headers);
        }
    }
}
