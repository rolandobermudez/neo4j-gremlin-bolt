package com.steelbridgelabs.oss.neo4j.structure;

class PropertyValueUtils {

    public static Object convertValueToSupportedType(Object value) {
        // check value is an Integer
        if (value instanceof Integer)
            return (long)(Integer)value;
        return value;
    }
}
