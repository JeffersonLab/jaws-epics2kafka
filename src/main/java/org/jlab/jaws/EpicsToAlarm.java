package org.jlab.jaws;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.jlab.jaws.entity.AlarmActivationUnion;
import org.jlab.jaws.entity.SevrEnum;
import org.jlab.jaws.entity.StatEnum;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Transforms records from epics2kafka to JAWS format.  Special hints are provided to ensure AVRO messages
 * honor enum and union types.
 *
 * @param <R> The Record to transform
 */
public abstract class EpicsToAlarm<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC =
            "Transform epics2kafka messages to JAWS format";

    public static final SevrEnum[] sevrByOrder = SevrEnum.values();
    public static final StatEnum[] statByOrder = StatEnum.values();

    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    private static final String PURPOSE = "transform epics2kafka messages to JAWS format";

    static AvroData inputData = new AvroData(new AvroDataConfig.Builder()
                .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
                .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, true)
                .build());

    static final Schema updatedValueSchema = inputData.toConnectSchema(AlarmActivationUnion.getClassSchema());

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

            String sevrStr = sevrByOrder[severity].name();
            String statStr = statByOrder[status].name();

            epicsMap.put("sevr", sevrStr);
            epicsMap.put("stat", statStr);

            msg.put("org.jlab.jaws.entity.EPICSAlarming", epicsMap);
            updated.put("msg", msg);

            return updated;
        }

        protected Struct doUpdate(Struct original, Schema updatedSchema) {
            Struct updated = new Struct(updatedSchema);
            Struct msg = new Struct(updatedSchema.field("msg").schema());

            Struct epicsStruct = new Struct(msg.schema().fields().get(2).schema());

            byte severity = original.getInt8("severity");
            byte status = original.getInt8("status");

            String sevrStr = sevrByOrder[severity].name();
            String statStr = statByOrder[status].name();

            epicsStruct.put("sevr", sevrStr);
            epicsStruct.put("stat", statStr);

            msg.put("org.jlab.jaws.entity.EPICSAlarming", epicsStruct);
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
