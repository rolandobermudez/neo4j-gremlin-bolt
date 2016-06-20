/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.steelbridgelabs.oss.neo4j.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Relationship;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Neo4JEdge extends Neo4JElement implements Edge {

    private class Neo4JEdgeProperty<T> implements Property<T> {

        private final String name;
        private final T value;

        public Neo4JEdgeProperty(String name, T value) {
            Objects.requireNonNull(name, "name cannot be null");
            Objects.requireNonNull(value, "value cannot be null");
            // store fields
            this.name = name;
            this.value = value;
        }

        @Override
        public String key() {
            return name;
        }

        @Override
        public T value() throws NoSuchElementException {
            return value;
        }

        @Override
        public boolean isPresent() {
            return true;
        }

        @Override
        public Element element() {
            return Neo4JEdge.this;
        }

        @Override
        public void remove() {
            // remove from vertex
            Neo4JEdge.this.properties.remove(name);
        }
    }

    private final Neo4JGraph graph;
    private final Neo4JSession session;
    private final Map<String, Neo4JEdgeProperty> properties = new HashMap<>();
    private final String idFieldName;
    private final Object id;
    private final String label;
    private final Neo4JVertex out;
    private final Neo4JVertex in;

    Neo4JEdge(Neo4JGraph graph, Neo4JSession session, String idFieldName, Object id, String label, Neo4JVertex out, Neo4JVertex in) {
        Objects.requireNonNull(graph, "graph cannot be null");
        Objects.requireNonNull(session, "session cannot be null");
        Objects.requireNonNull(idFieldName, "idFieldName cannot be null");
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(label, "label cannot be null");
        Objects.requireNonNull(properties, "properties cannot be null");
        Objects.requireNonNull(out, "out cannot be null");
        Objects.requireNonNull(in, "in cannot be null");
        // store fields
        this.graph = graph;
        this.session = session;
        this.idFieldName = idFieldName;
        this.id = id;
        this.label = label;
        this.out = out;
        this.in = in;
    }

    Neo4JEdge(Neo4JGraph graph, Neo4JSession session, String idFieldName, Neo4JVertex out, Relationship relationship, Neo4JVertex in) {
        Objects.requireNonNull(graph, "graph cannot be null");
        Objects.requireNonNull(session, "session cannot be null");
        Objects.requireNonNull(idFieldName, "idFieldName cannot be null");
        Objects.requireNonNull(out, "out cannot be null");
        Objects.requireNonNull(relationship, "relationship cannot be null");
        Objects.requireNonNull(in, "in cannot be null");
        // store fields
        this.graph = graph;
        this.session = session;
        this.idFieldName = idFieldName;
        // from relationship
        this.id = relationship.get(idFieldName).asObject();
        this.label = relationship.type();
        // copy properties from relationship, remove idFieldName from map
        StreamSupport.stream(relationship.keys().spliterator(), false).filter(key -> idFieldName.compareTo(key) != 0).forEach(key -> {
            // value
            Value value = relationship.get(key);
            // add property value
            properties.put(key, new Neo4JEdgeProperty<>(key, value.asObject()));
        });
        // vertices
        this.out = out;
        this.in = in;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        // out direction
        if (direction == Direction.OUT)
            return Stream.of((Vertex)out).iterator();
        // in direction
        if (direction == Direction.IN)
            return Stream.of((Vertex)in).iterator();
        // both
        return Stream.of((Vertex)out, in).iterator();
    }

    @Override
    public Object id() {
        return id;
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public <V> Property<V> property(String name, V value) {
        ElementHelper.validateProperty(name, value);
        // property value for key
        Neo4JEdgeProperty<V> propertyValue = new Neo4JEdgeProperty<>(name, value);
        // update map
        properties.put(name, propertyValue);
        // set edge as dirty
        session.dirtyEdge(this);
        // return property
        return propertyValue;
    }

    @Override
    public void remove() {
        // remove edge on session
        session.removeEdge(this, true);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        Objects.requireNonNull(propertyKeys, "propertyKeys cannot be null");
        // check filter is a single property
        if (propertyKeys.length == 1) {
            // property value
            Property<V> propertyValue = properties.get(propertyKeys[0]);
            if (propertyValue != null) {
                // return iterator
                return Collections.singleton(propertyValue).iterator();
            }
            return Collections.emptyIterator();
        }
        // no properties in filter
        if (propertyKeys.length == 0) {
            // all properties (return a copy since properties iterator can be modified by calling remove())
            return properties.values().stream()
                .map(value -> (Property<V>)value)
                .collect(Collectors.toList())
                .iterator();
        }
        // filter properties (return a copy since properties iterator can be modified by calling remove())
        return Arrays.stream(propertyKeys)
            .map(key -> (Property<V>)properties.get(key))
            .filter(property -> property != null)
            .collect(Collectors.toList())
            .iterator();
    }

    @Override
    public Map<String, Object> statementParameters() {
        // process properties
        Map<String, Object> parameters = properties.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().value()));
        // append id
        parameters.put(idFieldName, id);
        // return parameters
        return parameters;
    }

    @Override
    public Statement insertStatement() {
        // create statement
        String statement = String.format(Locale.US, "MATCH (o:%s{%s: {oid}}), (i:%s{%s: {iid}}) CREATE (o)-[r:`%s`{ep}]->(i)", Neo4JVertex.processLabels(out.labels()), idFieldName, Neo4JVertex.processLabels(in.labels()), idFieldName, label);
        // parameters
        Value parameters = Values.parameters("oid", out.id(), "iid", in.id(), "ep", statementParameters());
        // command statement
        return new Statement(statement, parameters);
    }

    @Override
    public Statement updateStatement() {
        // create statement
        String statement = String.format(Locale.US, "MATCH (o:%s{%s: {oid}}), (i:%s{%s: {iid}}) MERGE (o)-[r:`%s`{%s: {id}}]->(i) ON MATCH SET r = {rp}", Neo4JVertex.processLabels(out.labels()), idFieldName, Neo4JVertex.processLabels(in.labels()), idFieldName, label, idFieldName);
        // parameters
        Value parameters = Values.parameters("oid", out.id(), "iid", in.id(), idFieldName, id, "rp", statementParameters());
        // command statement
        return new Statement(statement, parameters);
    }

    @Override
    public Statement deleteStatement() {
        // create statement
        String statement = String.format(Locale.US, "MATCH (o:%s{%s: {oid}})-[r:`%s`{%s: {id}}]->(i:%s{%s: {iid}}) DELETE r",
            Neo4JVertex.processLabels(out.labels()), idFieldName,
            label,
            idFieldName,
            Neo4JVertex.processLabels(in.labels()), idFieldName);
        // parameters
        Value parameters = Values.parameters("oid", out.id(), "iid", in.id(), idFieldName, id);
        // command statement
        return new Statement(statement, parameters);
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }
}
