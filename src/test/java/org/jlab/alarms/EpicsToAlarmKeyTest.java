package org.jlab.alarms;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class EpicsToAlarmKeyTest {
    private EpicsToAlarm<SourceRecord> xform = new EpicsToAlarm.Key<>();

    public final Schema INPUT_KEY_SCHEMA = Schema.STRING_SCHEMA;

    @After
    public void teardown() {
        xform.close();
    }


    @Test
    public void tombstoneSchemaless() {

        String key = null;

        final SourceRecord record = new SourceRecord(null, null, null, null, key, null, null);
        final SourceRecord transformed = xform.apply(record);

        assertNull(transformed.key());
        assertNull(transformed.keySchema());
    }

    @Test
    public void tombstoneWithSchema() {

        final String key = null;

        final SourceRecord record = new SourceRecord(null, null, null, INPUT_KEY_SCHEMA, key, null, null);
        final SourceRecord transformed = xform.apply(record);

        assertNull(transformed.key());
        assertEquals(INPUT_KEY_SCHEMA, transformed.keySchema());
    }

    @Test
    public void schemaless() {
        String key = "channel1";

        final SourceRecord record = new SourceRecord(null, null, null, null, key, null, null);
        final SourceRecord transformed = xform.apply(record);

        Map transformedKey = (Map)transformed.key();

        assertEquals(key, transformedKey.get("name"));
        assertEquals("EPICSAlarming", transformedKey.get("type"));
    }

    @Test
    public void withSchema() {
        final String key = "channel1";

        final SourceRecord record = new SourceRecord(null, null, null, INPUT_KEY_SCHEMA, key, null, null);
        final SourceRecord transformed = xform.apply(record);

        Struct transformedKey = (Struct)transformed.key();

        assertEquals(key, transformedKey.getString("name"));
        assertEquals("EPICSAlarming", transformedKey.getString("type"));
    }

    @Test
    public void connectSchemaToAvroSchema() {
        AvroDataConfig config = new AvroDataConfig.Builder()
                .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
                .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
                .build();

        AvroData avroData = new AvroData(config);

        org.apache.kafka.connect.data.Schema connectSchema = EpicsToAlarm.updatedKeySchema;

        org.apache.avro.Schema actualAvroSchema = avroData.fromConnectSchema(connectSchema);

        org.apache.avro.Schema expectedAvroSchema = org.apache.avro.SchemaBuilder
                .builder()
                .record("org.jlab.alarms.ActiveAlarmKey")
                .doc("Active alarms state (alarming or acknowledgment)")
                .fields()
                .name("name").doc("The unique name of the alarm").type().stringType().noDefault()
                .name("type").doc("The type of message included in the value - required as part of the key to ensure compaction keeps the latest message of each type").type().enumeration("ActiveMessageType").namespace("org.jlab.alarms").doc("Enumeration of possible message types").symbols("SimpleAlarming", "SimpleAck", "EPICSAlarming", "EPICSAck").noDefault()
                .endRecord();

        //System.out.println("Actual:   " + actualAvroSchema);
        //System.out.println("Expected: " + expectedAvroSchema);

        // Schema objects weirdly say they're equal even if doc fields are wrong so we use string comparison
        assertEquals(expectedAvroSchema.toString(), actualAvroSchema.toString());
    }
}
