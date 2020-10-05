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

        final Map<String, Object> updatedMap = new HashMap<>();

        doUpdate(map, updatedMap);

        return newRecord(record, null, updatedMap);
    }

    private R applyWithSchema(R record) {
        System.err.println("applyWithSchema!");
        final Struct struct = requireStruct(operatingValue(record), PURPOSE);

        Schema schema = operatingSchema(record);

        final SchemaBuilder builder = SchemaBuilder.struct();

        // TODO: Don't re-build this EVERY time!
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

        builder.name("org.jlab.kafka.alarms.ActiveAlarm");
        builder.doc("Alarms currently alarming");
        builder.version(1);
        builder.field("priority", prioritySchema);
        builder.field("acknowledged", SchemaBuilder.bool().doc("Indicates whether this alarm has been explicitly acknowledged - useful for latching alarms which can only be cleared after acknowledgement").defaultValue(false).build());

        Schema updatedSchema = builder.build();

        final Struct updatedStruct = new Struct(updatedSchema);

        doUpdate(struct, updatedStruct);

        return newRecord(record, updatedSchema, updatedStruct);
    }

    private void doUpdate(Map original, Map updated) {
        System.err.println("map doUpdate!");
        updated.put("priority", "P4_DIAGNOSTIC");
        updated.put("acknowledged", false);
    }

    public void doUpdate(Struct original, Struct updated) {
        System.err.println("struct doUpdate!");
        updated.put("priority", "P4_DIAGNOSTIC");
        updated.put("acknowledged", false);
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
