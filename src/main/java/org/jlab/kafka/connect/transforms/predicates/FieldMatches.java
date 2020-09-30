package org.jlab.kafka.connect.transforms.predicates;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.transforms.util.RegexValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class FieldMatches<R extends ConnectRecord<R>> implements Predicate<R> {
    private static final String PURPOSE = "field matches predicate";
    private static final String FIELD_CONFIG = "field";
    private static final String PATTERN_CONFIG = "pattern";

    public static final String OVERVIEW_DOC = "A predicate which is true for records with a field value that matches the configured regular expression.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.CompositeValidator.of(new ConfigDef.NonEmptyString()),
                    ConfigDef.Importance.MEDIUM,
                    "A string containing the name of the field in which the field value will be examined")
            .define(PATTERN_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.CompositeValidator.of(new ConfigDef.NonEmptyString(), new RegexValidator()),
                    ConfigDef.Importance.MEDIUM,
                    "A Java regular expression for matching against the value of a record's field.");
    private String fieldName;
    private Pattern pattern;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public boolean test(R record) {
        if (operatingSchema(record) == null) {
            return testSchemaless(record);
        } else {
            return testWithSchema(record);
        }
    }

    private boolean testSchemaless(R record) {
        final Map<String, Object> map = requireMap(operatingValue(record), PURPOSE);

        Object value = map.get(fieldName);

        return testField(value);
    }

    private boolean testWithSchema(R record) {
        final Struct struct = requireStruct(operatingValue(record), PURPOSE);

        Object value = struct.get(fieldName);

        return testField(value);
    }

    private boolean testField(Object value) {
        if(value != null && value instanceof String) {
            String valueStr = (String)value;

            return pattern.matcher(valueStr).matches();
        }

        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig simpleConfig = new SimpleConfig(config(), configs);

        this.fieldName = simpleConfig.getString(FIELD_CONFIG);

        Pattern result;
        String value = simpleConfig.getString(PATTERN_CONFIG);
        try {
            result = Pattern.compile(value);
        } catch (PatternSyntaxException e) {
            throw new ConfigException(PATTERN_CONFIG, value, "entry must be a Java-compatible regular expression: " + e.getMessage());
        }
        this.pattern = result;
    }

    @Override
    public String toString() {
        return "FieldMatches{" +
                "fieldName=" + fieldName +
                ", pattern=" + pattern +
                '}';
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    public static class Key<R extends ConnectRecord<R>> extends FieldMatches<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends FieldMatches<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }
    }
}
