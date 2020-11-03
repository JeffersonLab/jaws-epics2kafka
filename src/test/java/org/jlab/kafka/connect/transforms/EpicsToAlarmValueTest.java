package org.jlab.kafka.connect.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.jlab.kafka.connect.transforms.util.SeverityEnum;
import org.jlab.kafka.connect.transforms.util.StatusEnum;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class EpicsToAlarmValueTest {
    private EpicsToAlarm<SourceRecord> xform = new EpicsToAlarm.Value<>();

    public final Schema INPUT_VALUE_SCHEMA = SchemaBuilder
            .struct()
            .name("org.jlab.epics.ca.value").version(1).doc("An EPICS Channel Access (CA) Time Database Record (DBR) MonitorEvent value")
            .field("timestamp", SchemaBuilder.int64().doc("UNIX timestamp (seconds from epoch - Jan. 1 1970 UTC less leap seconds)").build())
            .field("status", SchemaBuilder.int8().optional().doc("CA Alarm Status: 0=NO_ALARM,1=READ,2=WRITE,3=HIHI,4=HIGH,5=LOLO,6=LOW,7=STATE,8=COS,9=COMM,10=TIMEOUT,11=HW_LIMIT,12=CALC,13=SCAN,14=LINK,15=SOFT,16=BAD_SUB,17=UDF,18=DISABLE,19=SIMM,20=READ_ACCESS,21=WRITE_ACCESS").build())
            .field("severity", SchemaBuilder.int8().optional().doc("CA Alarm Severity: 0=NO_ALARM,1=MINOR,2=MAJOR,3=INVALID").build())
            .field("floatValues", SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
            .field("stringValues", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
            .field("intValues", SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build())
            .build();

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

        final Struct value = null;

        final SourceRecord record = new SourceRecord(null, null, null, null, null, INPUT_VALUE_SCHEMA, value);
        final SourceRecord transformed = xform.apply(record);

        assertNull(transformed.value());
        assertEquals(INPUT_VALUE_SCHEMA, transformed.valueSchema());
    }

    @Test
    public void schemaless() {
        Map<String, Object> value = new HashMap<>();

        value.put("severity", (byte)2);
        value.put("status", (byte)3);

        final SourceRecord record = new SourceRecord(null, null, null, null, null, null, value);
        final SourceRecord transformed = xform.apply(record);

        Map transformedValue = (Map)transformed.value();

        assertEquals(SeverityEnum.fromOrdinal((byte)2).name(), ((Map)transformedValue.get("msg")).get("sevr"));
        assertEquals(StatusEnum.fromOrdinal((byte)3).name(), ((Map)transformedValue.get("msg")).get("stat"));
    }

    @Test
    public void withSchema() {
        final Struct value = new Struct(INPUT_VALUE_SCHEMA);

        value.put("severity", (byte)2);
        value.put("status", (byte)3);

        final SourceRecord record = new SourceRecord(null, null, null, null, null, INPUT_VALUE_SCHEMA, value);
        final SourceRecord transformed = xform.apply(record);

        Struct transformedValue = (Struct)transformed.value();

        assertEquals(SeverityEnum.fromOrdinal((byte)2).name(), transformedValue.getStruct("msg").getStruct("AlarmingEPICS").getString("sevr"));
        assertEquals(StatusEnum.fromOrdinal((byte)3).name(), transformedValue.getStruct("msg").getStruct("AlarmingEPICS").getString("stat"));
    }
}
