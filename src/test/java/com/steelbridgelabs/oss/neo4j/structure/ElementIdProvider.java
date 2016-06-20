package com.steelbridgelabs.oss.neo4j.structure;

import java.util.concurrent.atomic.AtomicLong;

public class ElementIdProvider implements Neo4JElementIdProvider<Long> {

    public static String IdFieldName = "id";

    private final AtomicLong atomicLong = new AtomicLong();

    @Override
    public String idFieldName() {
        return IdFieldName;
    }

    @Override
    public Long generateId() {
        return atomicLong.incrementAndGet();
    }
}
