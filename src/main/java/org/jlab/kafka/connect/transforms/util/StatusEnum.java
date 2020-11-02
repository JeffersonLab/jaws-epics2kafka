package org.jlab.kafka.connect.transforms.util;

public enum StatusEnum {
    // WARNING: order is important here
    NO_ALARM, READ, WRITE, HIHI, LOLO, LOW, STATE, COS, COMM, TIMEOUT, HW_LIMIT, CALC, SCAN, LINK, SOFT, BAD_SUB, UDF, DISABLE, SIMM, READ_ACCESS, WRITE_ACCESS;

    private static StatusEnum[] values = StatusEnum.values();

    public static StatusEnum fromOrdinal(byte ordinal) {
        return values[ordinal];
    }
}
