package org.jlab.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class EpicsToAlarm<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC =
            "Transform epics2kafka messages to kafka-alarm-system format";

    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    private static final String PURPOSE = "transform epics2kafka messages to kafka-alarm-system format";

    final Schema prioritySchema = SchemaBuilder
            .string()
            .doc("Alarm severity organized as a way for operators to prioritize which alarms to take action on first")
            .parameter("io.confluent.connect.avro.enum.doc.AlarmPriority", "Enumeration of possible alarm priorities")
            .parameter("io.confluent.connect.avro.Enum", "org.jlab.kafka.alarms.AlarmPriority")
            .parameter("io.confluent.connect.avro.Enum.1", "P1_LIFE")
            .parameter("io.confluent.connect.avro.Enum.2", "P2_PROPERTY")
            .parameter("io.confluent.connect.avro.Enum.3", "P3_PRODUCTIVITY")
            .parameter("io.confluent.connect.avro.Enum.4", "P4_DIAGNOSTIC")
            .build();

    final Schema acknowledgedSchema = SchemaBuilder
            .bool()
            .doc("Indicates whether this alarm has been explicitly acknowledged - useful for latching alarms which can only be cleared after acknowledgement")
            .defaultValue(false)
            .build();

    final Schema updatedSchema = SchemaBuilder.struct()
            .name("org.jlab.kafka.alarms.ActiveAlarm")
            .doc("Alarms currently alarming")
            .version(1)
            .field("priority", prioritySchema)
            .field("acknowledged", acknowledgedSchema)
            .build();

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

        final Struct updatedStruct = doUpdate(struct, updatedSchema);

        return newRecord(record, updatedSchema, updatedStruct);
    }

    private Map<String, Object> doUpdate(Map<String, Object> original) {
        System.err.println("map doUpdate!");

        Map<String, Object> updated = null;
        Object priority = null;

        byte severity = (byte)original.get("severity");

        System.err.println("severity: " + severity);


        if(severity == 1 || severity == 2) { // MINOR or MAJOR
            priority = "P4_DIAGNOSTIC";
        } // 0 and 3 mean NO_ALARM or INVALID, so we use null

        if(priority != null) {
            updated = new HashMap<>();
            updated.put("priority", priority);
            updated.put("acknowledged", false);
        }

        return updated;
    }

    public Struct doUpdate(Struct original, Schema updatedSchema) {
        System.err.println("struct doUpdate!");

        Struct updated = null;
        Object priority = null;

        byte severity = original.getInt8("severity");

        System.err.println("severity: " + severity);

        if(severity == 1 || severity == 2) { // MINOR or MAJOR
            priority = "P4_DIAGNOSTIC";
        } // 0 and 3 mean NO_ALARM or INVALID, so we use null

        if(priority != null) {
            updated = new Struct(updatedSchema);
            updated.put("priority", priority);
            updated.put("acknowledged", false);
        }

        return updated;
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

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends EpicsToAlarm<R> {

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
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends EpicsToAlarm<R> {

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
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}
