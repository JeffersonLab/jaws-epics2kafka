package org.jlab.alarms.util;

public enum SeverityEnum {
    // WARNING: order is important here
    NO_ALARM, MINOR, MAJOR, INVALID;

    private static SeverityEnum[] values = SeverityEnum.values();

    public static SeverityEnum fromOrdinal(byte ordinal) {
        return values[ordinal];
    }
}
