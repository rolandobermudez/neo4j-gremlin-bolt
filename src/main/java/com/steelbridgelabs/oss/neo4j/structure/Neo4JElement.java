package com.steelbridgelabs.oss.neo4j.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.neo4j.driver.v1.Statement;

import java.util.Map;

abstract class Neo4JElement implements Element {

    public abstract Map<String, Object> statementParameters();

    public abstract Statement insertStatement();

    public abstract Statement updateStatement();

    public abstract Statement deleteStatement();

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }
}
