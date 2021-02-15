package org.jlab.alarms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.jlab.alarms.util.SeverityEnum;
import org.jlab.alarms.util.StatusEnum;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Transforms records from epics2kafka to kafka-alarm-system format.  Special hints are provided to ensure AVRO messages
 * honor enum and union types.
 *
 * @param <R> The Record to transform
 */
public abstract class EpicsToAlarm<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC =
            "Transform epics2kafka messages to kafka-alarm-system format";

    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    private static final String PURPOSE = "transform epics2kafka messages to kafka-alarm-system format";

    static final Schema sevrSchema = SchemaBuilder
            .string()
            .name("org.jlab.alarms.SevrEnum")
            .doc("Alarming state (EPICS .SEVR field)")
            .parameter("io.confluent.connect.avro.enum.doc.SevrEnum", "Enumeration of possible EPICS .SEVR values")
            .parameter("io.confluent.connect.avro.Enum", "org.jlab.alarms.SevrEnum")
            .parameter("io.confluent.connect.avro.Enum.1", "NO_ALARM")
            .parameter("io.confluent.connect.avro.Enum.2", "MINOR")
            .parameter("io.confluent.connect.avro.Enum.3", "MAJOR")
            .parameter("io.confluent.connect.avro.Enum.4", "INVALID")
            .build();

    static final Schema statSchema = SchemaBuilder
            .string()
            .name("org.jlab.alarms.StatEnum")
            .doc("Alarming status (EPICS .STAT field)")
            .parameter("io.confluent.connect.avro.enum.doc.StatEnum", "Enumeration of possible EPICS .STAT values")
            .parameter("io.confluent.connect.avro.Enum", "org.jlab.alarms.StatEnum")
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

    static final Schema ackEnumSchema = SchemaBuilder
            .string()
            .name("org.jlab.alarms.EPICSAcknowledgementEnum")
            .doc("Indicates whether this alarm has been explicitly acknowledged - useful for latching alarms which can only be cleared after acknowledgement")
            .parameter("io.confluent.connect.avro.enum.doc.EPICSAcknowledgementEnum", "Enumeration of possible EPICS acknowledgement states")
            .parameter("io.confluent.connect.avro.Enum", "org.jlab.alarms.EPICSAcknowledgementEnum")
            .parameter("io.confluent.connect.avro.Enum.1", "NO_ACK")
            .parameter("io.confluent.connect.avro.Enum.2", "MINOR_ACK")
            .parameter("io.confluent.connect.avro.Enum.3", "MAJOR_ACK")
            .build();

    static final Schema typeSchema = SchemaBuilder
            .string()
            .name("org.jlab.alarms.ActiveMessageType")
            .doc("The type of message included in the value - required as part of the key to ensure compaction keeps the latest message of each type")
            .parameter("io.confluent.connect.avro.enum.doc.ActiveMessageType", "Enumeration of possible message types")
            .parameter("io.confluent.connect.avro.Enum", "org.jlab.alarms.ActiveMessageType")
            .parameter("io.confluent.connect.avro.Enum.1", "SimpleAlarming")
            .parameter("io.confluent.connect.avro.Enum.2", "SimpleAck")
            .parameter("io.confluent.connect.avro.Enum.3", "EPICSAlarming")
            .parameter("io.confluent.connect.avro.Enum.4", "EPICSAck")
            .build();

    static final Schema alarmingSchema = SchemaBuilder
            .struct()
            .optional()
            .name("org.jlab.alarms.SimpleAlarming")
            .doc("Alarming state for a simple alarm, if record is present then alarming, if missing/tombstone then not.  There are no fields.")
            .parameter("io.confluent.connect.avro.record.doc", "Alarming state for a simple alarm, if record is present then alarming, if missing/tombstone then not.  There are no fields.")
            .build();

    static final Schema ackSchema = SchemaBuilder
            .struct()
            .optional()
            .name("org.jlab.alarms.SimpleAck")
            .doc("A simple acknowledgment message, if record is present then acknowledged, if missing/tombstone then not.  There are no fields")
            .parameter("io.confluent.connect.avro.record.doc", "A simple acknowledgment message, if record is present then acknowledged, if missing/tombstone then not.  There are no fields")
            .build();

    static final Schema alarmingEPICSSchema = SchemaBuilder
            .struct()
            .optional()
            .name("org.jlab.alarms.EPICSAlarming")
            .doc("EPICS alarming state")
            .parameter("io.confluent.connect.avro.record.doc", "EPICS alarming state")
            .parameter("io.confluent.connect.avro.field.doc.sevr","Alarming state (EPICS .SEVR field)")
            .parameter("io.confluent.connect.avro.field.doc.stat","Alarming status (EPICS .STAT field)")
            .field("sevr", sevrSchema)
            .field("stat", statSchema)
            .build();

    static final Schema ackEPICSSchema = SchemaBuilder
            .struct()
            .optional()
            .name("org.jlab.alarms.EPICSAck")
            .doc("EPICS acknowledgement state")
            .parameter("io.confluent.connect.avro.record.doc", "EPICS acknowledgement state")
            .parameter("io.confluent.connect.avro.field.doc.ack","Indicates whether this alarm has been explicitly acknowledged - useful for latching alarms which can only be cleared after acknowledgement")
            .field("ack", ackEnumSchema)
            .build();

    static final Schema msgSchema = SchemaBuilder
            .struct()
            .name("io.confluent.connect.avro.Union")
            .doc("Two types of messages are allowed: Alarming and Acknowledgement; There can be multiple flavors of each type for different alarm producers; modeled as a nested union to avoid complications of union at root of schema.")
            .field("SimpleAlarming", alarmingSchema)
            .field("SimpleAck", ackSchema)
            .field("EPICSAlarming", alarmingEPICSSchema)
            .field("EPICSAck", ackEPICSSchema)
            .build();

    static final Schema updatedValueSchema = SchemaBuilder
            .struct()
            .name("org.jlab.alarms.ActiveAlarmValue")
            .doc("Alarming and Acknowledgements state")
            .parameter("io.confluent.connect.avro.record.doc", "Alarming and Acknowledgements state")
            .parameter("io.confluent.connect.avro.field.doc.msg","Two types of messages are allowed: Alarming and Acknowledgement; There can be multiple flavors of each type for different alarm producers; modeled as a nested union to avoid complications of union at root of schema.")
            .version(1)
            .field("msg", msgSchema)
            .build();

    static final Schema updatedKeySchema = SchemaBuilder
            .struct()
            .name("org.jlab.alarms.ActiveAlarmKey")
            .doc("Active alarms state (alarming or acknowledgment)")
            .parameter("io.confluent.connect.avro.record.doc", "Active alarms state (alarming or acknowledgment)")
            .parameter("io.confluent.connect.avro.field.doc.name", "The unique name of the alarm")
            .parameter("io.confluent.connect.avro.field.doc.type", "The type of message included in the value - required as part of the key to ensure compaction keeps the latest message of each type")
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
        headers.addString("producer", "epics2kafka-alarms");
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
        if(operatingValue(record) == null) {
            return record; // tombstone value or null key
        } else if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
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

    protected abstract R applySchemaless(R record);

    protected abstract R applyWithSchema(R record);

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends EpicsToAlarm<R> {

        @Override
        protected R applySchemaless(R record) {
            final String original = record.key().toString();

            final Map<String, Object> updatedMap = doUpdate(original);

            return newRecord(record, null, updatedMap);
        }

        @Override
        protected R applyWithSchema(R record) {
            final String original = record.key().toString();

            final Struct updatedStruct = doUpdate(original, updatedKeySchema);

            return newRecord(record, updatedKeySchema, updatedStruct);
        }

        protected Map<String, Object> doUpdate(String original) {
            Map<String, Object> updated = new HashMap<>();

            updated.put("name", original);
            updated.put("type", "EPICSAlarming");

            return updated;
        }

        protected Struct doUpdate(String original, Schema updatedSchema) {
            Struct updated = new Struct(updatedSchema);

            updated.put("name", original);
            updated.put("type", "EPICSAlarming");

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
        protected R applySchemaless(R record) {
            final Map<String, Object> map = requireMap(operatingValue(record), PURPOSE);

            final Map<String, Object> updatedMap = doUpdate(map);

            return newRecord(record, null, updatedMap);
        }

        @Override
        protected R applyWithSchema(R record) {
            final Struct struct = requireStruct(operatingValue(record), PURPOSE);

            final Struct updatedStruct = doUpdate(struct, updatedValueSchema);

            return newRecord(record, updatedValueSchema, updatedStruct);
        }

        protected Map<String, Object> doUpdate(Map<String, Object> original) {
            Map<String, Object> updated = new HashMap<>();
            Map<String, Object> msg = new HashMap<>();
            Map<String, Object> epicsMap = new HashMap<>();

            byte severity = (byte)original.get("severity");
            byte status = (byte)original.get("status");

            String sevrStr = SeverityEnum.fromOrdinal(severity).name();
            String statStr = StatusEnum.fromOrdinal(status).name();

            epicsMap.put("sevr", sevrStr);
            epicsMap.put("stat", statStr);

            msg.put("EPICSAlarming", epicsMap);
            updated.put("msg", msg);

            return updated;
        }

        protected Struct doUpdate(Struct original, Schema updatedSchema) {
            Struct updated = new Struct(updatedSchema);
            Struct msg = new Struct(msgSchema);
            Struct epicsStruct = new Struct(alarmingEPICSSchema);

            byte severity = original.getInt8("severity");
            byte status = original.getInt8("status");

            String sevrStr = SeverityEnum.fromOrdinal(severity).name();
            String statStr = StatusEnum.fromOrdinal(status).name();

            epicsStruct.put("sevr", sevrStr);
            epicsStruct.put("stat", statStr);

            msg.put("EPICSAlarming", epicsStruct);
            updated.put("msg", msg);

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
