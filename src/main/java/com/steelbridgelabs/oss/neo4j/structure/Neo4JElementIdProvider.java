package com.steelbridgelabs.oss.neo4j.structure;

public interface Neo4JElementIdProvider<T> {

    String idFieldName();

    T generateId();
}
