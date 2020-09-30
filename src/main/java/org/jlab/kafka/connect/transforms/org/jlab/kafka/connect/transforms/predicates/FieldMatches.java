package org.jlab.kafka.connect.transforms.org.jlab.kafka.connect.transforms.predicates;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.transforms.util.RegexValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.jlab.kafka.connect.transforms.Substitute;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class FieldMatches<R extends ConnectRecord<R>> implements Predicate<R> {
    private static final String PURPOSE = "field matches predicate";
    private static final String PATTERN_CONFIG = "pattern";
    private static final String FIELD_CONFIG = "field";

    public static final String OVERVIEW_DOC = "A predicate which is true for records with a field value that matches the configured regular expression.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(PATTERN_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.CompositeValidator.of(new ConfigDef.NonEmptyString(), new RegexValidator()),
                    ConfigDef.Importance.MEDIUM,
                    "A Java regular expression for matching against the value of a record's field.")
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.CompositeValidator.of(new ConfigDef.NonEmptyString()),
                    ConfigDef.Importance.MEDIUM,
                    "A string containing the name of the field in which the field value will be examined");
    private Pattern pattern;
    private String fieldName;

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
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        Object originalValue = value.get(fieldName);

        if(originalValue != null && originalValue instanceof String) {
            String originalString = (String)originalValue;

            return pattern.matcher(originalString).matches();
        }

        return false;
    }

    private boolean testWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Object originalValue = value.get(fieldName);

        if(originalValue != null && originalValue instanceof String) {
            String originalString = (String)originalValue;

            return pattern.matcher(originalString).matches();
        }

        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig simpleConfig = new SimpleConfig(config(), configs);
        Pattern result;
        String value = simpleConfig.getString(PATTERN_CONFIG);
        try {
            result = Pattern.compile(value);
        } catch (PatternSyntaxException e) {
            throw new ConfigException(PATTERN_CONFIG, value, "entry must be a Java-compatible regular expression: " + e.getMessage());
        }
        this.pattern = result;

        this.fieldName = simpleConfig.getString(FIELD_CONFIG);
    }

    @Override
    public String toString() {
        return "FieldMatches{" +
                "pattern=" + pattern +
                ", fieldName=" + fieldName +
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
