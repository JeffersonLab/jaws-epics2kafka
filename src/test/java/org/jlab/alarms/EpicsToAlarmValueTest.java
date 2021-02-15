package org.jlab.alarms;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.jlab.alarms.util.SeverityEnum;
import org.jlab.alarms.util.StatusEnum;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class EpicsToAlarmValueTest {
    private EpicsToAlarm<SourceRecord> xform = new EpicsToAlarm.Value<>();

    public final Schema INPUT_VALUE_SCHEMA = SchemaBuilder.struct()
            .name("org.jlab.kafka.connect.EPICS_CA_DBR").version(1).doc("An EPICS Channel Access (CA) Time Database Record (DBR) MonitorEvent value")
            .field("timestamp", SchemaBuilder.int64().doc("UNIX timestamp (seconds from epoch - Jan. 1 1970 UTC less leap seconds)").build())
            .field("status", SchemaBuilder.int8().optional().doc("CA Alarm Status: 0=NO_ALARM,1=READ,2=WRITE,3=HIHI,4=HIGH,5=LOLO,6=LOW,7=STATE,8=COS,9=COMM,10=TIMEOUT,11=HW_LIMIT,12=CALC,13=SCAN,14=LINK,15=SOFT,16=BAD_SUB,17=UDF,18=DISABLE,19=SIMM,20=READ_ACCESS,21=WRITE_ACCESS").build())
            .field("severity", SchemaBuilder.int8().optional().doc("CA Alarm Severity: 0=NO_ALARM,1=MINOR,2=MAJOR,3=INVALID").build())
            .field("doubleValues", SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().doc("EPICS DBR_DOUBLE").build())
            .field("floatValues", SchemaBuilder.array(Schema.OPTIONAL_FLOAT32_SCHEMA).optional().doc("EPICS DBR_FLOAT").build())
            .field("stringValues", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().doc("EPICS DBR_STRING").build())
            .field("intValues", SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().doc("EPICS DBR_LONG; JCA refers to INT32 as DBR_INT; EPICS has no INT64").build())
            .field("shortValues", SchemaBuilder.array(Schema.OPTIONAL_INT16_SCHEMA).optional().doc("EPICS DBR_SHORT; DBR_INT is alias in EPICS (but not in JCA); Schema has no unsigned types so DBR_ENUM is also here").build())
            .field("byteValues", SchemaBuilder.array(Schema.OPTIONAL_INT8_SCHEMA).optional().doc("EPICS DBR_CHAR").build())
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

        Map msg = (Map)transformedValue.get("msg");

        assertEquals(SeverityEnum.fromOrdinal((byte)2).name(), ((Map)msg.get("EPICSAlarming")).get("sevr"));
        assertEquals(StatusEnum.fromOrdinal((byte)3).name(), ((Map)msg.get("EPICSAlarming")).get("stat"));
    }

    @Test
    public void withSchema() {
        final Struct value = new Struct(INPUT_VALUE_SCHEMA);

        value.put("severity", (byte)2);
        value.put("status", (byte)3);

        final SourceRecord record = new SourceRecord(null, null, null, null, null, INPUT_VALUE_SCHEMA, value);
        final SourceRecord transformed = xform.apply(record);

        Struct transformedValue = (Struct)transformed.value();

        Struct msg = transformedValue.getStruct("msg");

        assertEquals(SeverityEnum.fromOrdinal((byte)2).name(), msg.getStruct("EPICSAlarming").getString("sevr"));
        assertEquals(StatusEnum.fromOrdinal((byte)3).name(), msg.getStruct("EPICSAlarming").getString("stat"));
    }

    @Test
    public void connectSchemaToAvroSchema() {
        AvroDataConfig config = new AvroDataConfig.Builder()
                .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
                .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
                .build();

        AvroData avroData = new AvroData(config);

        org.apache.kafka.connect.data.Schema connectSchema = EpicsToAlarm.updatedValueSchema;

        org.apache.avro.Schema actualAvroSchema = avroData.fromConnectSchema(connectSchema);

        org.apache.avro.Schema expectedAvroSchema = org.apache.avro.SchemaBuilder
                .builder()
                .record("org.jlab.alarms.ActiveAlarmValue")
                .doc("Alarming and Acknowledgements state")
                .fields()
                .name("msg").doc("Two types of messages are allowed: Alarming and Acknowledgement; There can be multiple flavors of each type for different alarm producers; modeled as a nested union to avoid complications of union at root of schema.").type().unionOf()
                    .record("org.jlab.alarms.SimpleAlarming").doc("Alarming state for a simple alarm, if record is present then alarming, if missing/tombstone then not.  There are no fields.").fields().endRecord()
                    .and()
                    .record("org.jlab.alarms.SimpleAck").doc("A simple acknowledgment message, if record is present then acknowledged, if missing/tombstone then not.  There are no fields").fields().endRecord()
                    .and()
                    .record("org.jlab.alarms.EPICSAlarming").doc("EPICS alarming state").fields()
                        .name("sevr").doc("Alarming state (EPICS .SEVR field)").type().enumeration("SevrEnum").doc("Enumeration of possible EPICS .SEVR values").symbols("NO_ALARM","MINOR","MAJOR","INVALID").noDefault()
                        .name("stat").doc("Alarming status (EPICS .STAT field)").type().enumeration("StatEnum").doc("Enumeration of possible EPICS .STAT values").symbols("NO_ALARM","READ","WRITE","HIHI","HIGH","LOLO","LOW","STATE","COS","COMM","TIMEOUT","HW_LIMIT","CALC","SCAN","LINK","SOFT","BAD_SUB","UDF","DISABLE","SIMM","READ_ACCESS","WRITE_ACCESS").noDefault()
                    .endRecord()
                    .and()
                    .record("org.jlab.alarms.EPICSAck").doc("EPICS acknowledgement state").fields()
                        .name("ack").doc("Indicates whether this alarm has been explicitly acknowledged - useful for latching alarms which can only be cleared after acknowledgement").type().enumeration("EPICSAcknowledgementEnum").doc("Enumeration of possible EPICS acknowledgement states").symbols("NO_ACK","MINOR_ACK", "MAJOR_ACK").noDefault()
                    .endRecord()
                .endUnion().noDefault()
                .endRecord();

        //System.out.println("Expected : " + expectedAvroSchema);
        //System.out.println("Actual   : " + actualAvroSchema);

        // Schema objects weirdly say they're equal even if doc fields are wrong so we use string comparison
        assertEquals(expectedAvroSchema.toString(), actualAvroSchema.toString());
    }
}
