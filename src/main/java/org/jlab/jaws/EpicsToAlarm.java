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

    public static final SevrEnum[] sevrByOrder = new SevrEnum[4];

    // We removed NO_ALARM since it cannot exist downstream as it's replaced with either a tombstone or NoActivation
    // marker.  However, incoming records have it, so we can no longer simply assign SevrEnum.values() as is done
    // with StateEnum.
    static {
        sevrByOrder[0] = null; // NO_ALARM
        sevrByOrder[1] = SevrEnum.MINOR;
        sevrByOrder[2] = SevrEnum.MAJOR;
        sevrByOrder[3] = SevrEnum.INVALID;
    }
    public static final StatEnum[] statByOrder = StatEnum.values();

    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    private static final String PURPOSE = "transform epics2kafka messages to JAWS format";

    static AvroData inputData = new AvroData(new AvroDataConfig.Builder()
                .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
                .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, true)
                .build());

    static final Schema updatedValueSchema = inputData.toConnectSchema(AlarmActivationUnion.getClassSchema());

    static boolean USE_NO_ACTIVATION = !"false".equals(System.getenv("USE_NO_ACTIVATION"));

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
        headers.addString("producer", "jaws-epics2kafka");
    }

    /**
     * This transform can either use tombstones to indicate no activation or use the NoActivation marker entity.
     *
     * The USE_NO_ACTIVATION environment variable is consulted and unless the value == "false", the default is to use
     * the NoActivation marker.
     *
     * The advantage of the marker is that it can be aggressively compacted and maintains the number of messages in the
     * topic at roughly equal to the number of registered alarms IF aggressive compaction is configured.  The downside
     * is the number of messages can never be less than registered alarms.   Best for workloads with spontaneous
     * nuisance alarms.
     *
     * The advantage of the tombstone is that in well-behaved alarm systems (few nuisance alarms), the number of
     * messages in the alarm-activations topic can be quite low.   The disadvantage is that tombstones cannot be
     * compacted and only expire based on delete.retention.ms, which means if you have a lot of toggling alarms
     * (nuisance alarms), then they are only cleaned up based on delete.retention.ms, which may need to be quite long
     * since that effectively caps the amount of time that can elapse before a materialized view must check for updates
     * so that it can incrementally catch-up to the latest state, otherwise the view must be entirely rebuilt from
     * scratch (re-query entire topic) to avoid missing deleted tombstones (compaction keeps the latest state of a topic
     * EXCEPT tombstones).
     *
     * @param useNoActivation true to use NoActivation marker, false to use tombstone (null)
     */
    public static void setUseNoActivation(boolean useNoActivation) {
        USE_NO_ACTIVATION = useNoActivation;
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
            Map<String, Object> updated = null;
            Map<String, Object> union = new HashMap<>();
            boolean alarm = true;

            String error = (String)original.get("error");

            if(error != null) {
                Map<String, Object> errorMap = new HashMap<>();

                errorMap.put("error", error);

                union.put("org.jlab.jaws.entity.ChannelErrorActivation", errorMap);

            } else {
                Map<String, Object> epicsMap = new HashMap<>();

                byte status = (byte) original.get("status");
                StatEnum stat = statByOrder[status];
                String statStr = stat.name();

                if(StatEnum.NO_ALARM.equals(stat)) {
                    alarm = false;
                } else {
                    byte severity = (byte) original.get("severity");
                    String sevrStr = sevrByOrder[severity].name();

                    epicsMap.put("sevr", sevrStr);
                    epicsMap.put("stat", statStr);

                    union.put("org.jlab.jaws.entity.EPICSActivation", epicsMap);
                }
            }

            if(!alarm && USE_NO_ACTIVATION) {
                union.put("org.jlab.jaws.entity.NoActivation", new HashMap<>());
            }

            if(alarm || USE_NO_ACTIVATION) {
                updated = new HashMap<>();
                updated.put("union", union);
            }

            return updated;
        }

        protected Struct doUpdate(Struct original, Schema updatedSchema) {
            Struct updated = null;
            Struct union = new Struct(updatedSchema.field("union").schema());
            boolean alarm = true;

            String error = original.getString("error");

            if(error != null) {
                Struct errorStruct = new Struct(union.schema().fields().get(3).schema());

                errorStruct.put("error", error);

                union.put("org.jlab.jaws.entity.ChannelErrorActivation", errorStruct);
            } else {

                Struct epicsStruct = new Struct(union.schema().fields().get(2).schema());

                byte status = original.getInt8("status");
                StatEnum stat = statByOrder[status];
                String statStr = stat.name();

                if(StatEnum.NO_ALARM.equals(stat)) {
                    alarm = false;
                } else {
                    byte severity = original.getInt8("severity");
                    String sevrStr = sevrByOrder[severity].name();

                    epicsStruct.put("sevr", sevrStr);
                    epicsStruct.put("stat", statStr);

                    union.put("org.jlab.jaws.entity.EPICSActivation", epicsStruct);
                }
            }

            if(!alarm && USE_NO_ACTIVATION) {
                union.put("org.jlab.jaws.entity.NoActivation",
                        new Struct(union.schema().fields().get(4).schema()));
            }

            if(alarm || USE_NO_ACTIVATION) {
                updated = new Struct(updatedSchema);
                updated.put("union", union);
            }

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
            if(updatedValue == null) {
                updatedSchema = null; // Otherwise Schema must be union including null
            }
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp(), headers);
        }
    }
}
