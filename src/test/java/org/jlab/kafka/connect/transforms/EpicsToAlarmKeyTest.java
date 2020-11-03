package org.jlab.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.jlab.kafka.connect.transforms.util.SeverityEnum;
import org.jlab.kafka.connect.transforms.util.StatusEnum;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
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
        assertEquals("AlarmingEPICS", transformedKey.get("type"));
    }

    @Test
    public void withSchema() {
        final String key = "channel1";

        final SourceRecord record = new SourceRecord(null, null, null, INPUT_KEY_SCHEMA, key, null, null);
        final SourceRecord transformed = xform.apply(record);

        Struct transformedKey = (Struct)transformed.key();

        assertEquals(key, transformedKey.getString("name"));
        assertEquals("AlarmingEPICS", transformedKey.getString("type"));
    }
}
