package org.jlab.kafka.connect.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class EpicsToAlarmValueTest {
    private EpicsToAlarm<SourceRecord> xform = new EpicsToAlarm.Value<>();

    @After
    public void teardown() {
        xform.close();
    }


    @Test
    public void tombstoneSchemaless() {

        Map<String, Object> value = null;

        final SourceRecord record = new SourceRecord(null, null, null, null, null, null, value);
        final SourceRecord transformed = xform.apply(record);

        assertNull(transformed.value());
        assertNull(transformed.valueSchema());
    }

    @Test
    public void tombstoneWithSchema() {

        final Schema schema = SchemaBuilder
                .struct()
                .field("testField", Schema.STRING_SCHEMA)
                .build();

        final Struct value = null;

        final SourceRecord record = new SourceRecord(null, null, null, null, null, schema, value);
        final SourceRecord transformed = xform.apply(record);

        assertNull(transformed.value());
        assertEquals(schema, transformed.valueSchema());
    }

    @Test
    public void schemaless() {

    }

    @Test
    public void withSchema() {

    }
}
