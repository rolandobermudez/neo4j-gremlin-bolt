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
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Rogelio J. Baucells
 */
class Neo4JSession {

    private static final Logger logger = LoggerFactory.getLogger(Neo4JSession.class);

    public static final String VertexLabelDelimiter = "::";

    private final Neo4JGraph graph;
    private final Session session;
    private final Neo4JElementIdProvider<?> vertexIdProvider;
    private final Neo4JElementIdProvider<?> edgeIdProvider;
    private final Neo4JElementIdProvider<?> propertyIdProvider;

    private final Map<Object, Neo4JVertex> vertices = new HashMap<>();
    private final Map<Object, Neo4JEdge> edges = new HashMap<>();
    private final Set<Object> deletedVertices = new HashSet<>();
    private final Set<Object> deletedEdges = new HashSet<>();
    private final Map<Object, Set<Neo4JEdge>> relations = new HashMap<>();
    private final Set<Neo4JVertex> transientVertices = new HashSet<>();
    private final Set<Neo4JEdge> transientEdges = new HashSet<>();
    private final Set<Neo4JVertex> vertexUpdateQueue = new HashSet<>();
    private final Set<Neo4JEdge> edgeUpdateQueue = new HashSet<>();
    private final Set<Neo4JVertex> vertexDeleteQueue = new HashSet<>();
    private final Set<Neo4JEdge> edgeDeleteQueue = new HashSet<>();
    private final String vertexIdFieldName;
    private final String edgeIdFieldName;

    private org.neo4j.driver.v1.Transaction transaction;
    private boolean verticesLoaded = false;
    private boolean edgesLoaded = false;

    public Neo4JSession(Neo4JGraph graph, Session session, Neo4JElementIdProvider<?> vertexIdProvider, Neo4JElementIdProvider<?> edgeIdProvider, Neo4JElementIdProvider<?> propertyIdProvider) {
        Objects.requireNonNull(graph, "graph cannot be null");
        Objects.requireNonNull(session, "session cannot be null");
        Objects.requireNonNull(vertexIdProvider, "vertexIdProvider cannot be null");
        Objects.requireNonNull(edgeIdProvider, "edgeIdProvider cannot be null");
        Objects.requireNonNull(propertyIdProvider, "propertyIdProvider cannot be null");
        // log information
        if (logger.isDebugEnabled())
            logger.debug("Creating session [{}]", session.hashCode());
        // store fields
        this.graph = graph;
        this.session = session;
        this.vertexIdProvider = vertexIdProvider;
        this.edgeIdProvider = edgeIdProvider;
        this.propertyIdProvider = propertyIdProvider;
        // initialize field ids names
        vertexIdFieldName = vertexIdProvider.idFieldName();
        edgeIdFieldName = edgeIdProvider.idFieldName();
    }

    public org.neo4j.driver.v1.Transaction beginTransaction() {
        // check we have a transaction already in progress
        if (transaction != null)
            throw Transaction.Exceptions.transactionAlreadyOpen();
        // log information
        if (logger.isDebugEnabled())
            logger.debug("Beginning transaction on session [{}]", session.hashCode());
        // begin transaction
        transaction = session.beginTransaction();
        // return transaction instance
        return transaction;
    }

    public Vertex addVertex(Object[] keyValues) {
        Objects.requireNonNull(keyValues, "keyValues cannot be null");
        // verify parameters are key/value pairs
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        // id cannot be present
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();
        // open transaction if needed
        Transaction transaction = graph.tx();
        if (!transaction.isOpen())
            transaction.open();
        // create vertex
        Neo4JVertex vertex = new Neo4JVertex(graph, this, propertyIdProvider, vertexIdFieldName, vertexIdProvider.generateId(), Arrays.asList(ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL).split(VertexLabelDelimiter)));
        // add vertex to transient set (before processing properties to avoid having a transient vertex in update queue)
        transientVertices.add(vertex);
        // attach properties
        ElementHelper.attachProperties(vertex, keyValues);
        // register element
        registerVertex(vertex);
        // return vertex
        return vertex;
    }

    Edge addEdge(String label, Neo4JVertex out, Neo4JVertex in, Object[] keyValues) {
        Objects.requireNonNull(label, "label cannot be null");
        Objects.requireNonNull(out, "out cannot be null");
        Objects.requireNonNull(in, "in cannot be null");
        Objects.requireNonNull(keyValues, "keyValues cannot be null");
        // validate label
        ElementHelper.validateLabel(label);
        // verify parameters are key/value pairs
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        // id cannot be present
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();
        // open transaction if needed
        Transaction transaction = graph.tx();
        if (!transaction.isOpen())
            transaction.open();
        // create edge
        Neo4JEdge edge = new Neo4JEdge(graph, this, edgeIdFieldName, edgeIdProvider.generateId(), label, out, in);
        // register transient edge (before processing properties to avoid having a transient edge in update queue)
        transientEdges.add(edge);
        // attach properties
        ElementHelper.attachProperties(edge, keyValues);
        // register element
        registerEdge(edge);
        // register transient edge with adjacent vertices
        out.addOutEdge(edge);
        in.addInEdge(edge);
        // return edge
        return edge;
    }

    public Iterator<Vertex> vertices(Object[] ids) {
        Objects.requireNonNull(ids, "ids cannot be null");
        // verify identifiers
        verifyIdentifiers(Vertex.class, ids);
        // check we have all vertices already loaded
        if (!verticesLoaded) {
            // check ids
            if (ids.length > 0) {
                // parameters as a stream
                Set<Object> identifiers = Arrays.stream(ids).map(Neo4JSession::processIdentifier).collect(Collectors.toSet());
                // filter ids, remove ids already in memory (only ids that might exist on server)
                List<Object> filter = identifiers.stream().filter(id -> !vertices.containsKey(id)).collect(Collectors.toList());
                // check we need to execute statement in server
                if (!filter.isEmpty()) {
                    // cypher statement
                    Statement statement = new Statement(String.format(Locale.US, "MATCH (n) WHERE n.%s in {ids} RETURN n", vertexIdFieldName), Values.parameters("ids", filter));
                    // create stream from query
                    Stream<Vertex> query = vertices(statement);
                    // combine stream from memory and query result
                    return combine(identifiers.stream().filter(vertices::containsKey).map(id -> (Vertex)vertices.get(id)), query);
                }
                // no need to execute query, only items in memory
                return combine(identifiers.stream().filter(vertices::containsKey).map(id -> (Vertex)vertices.get(id)), Stream.empty());
            }
            // ids in memory (only ids that might exist on server)
            List<Object> filter = vertices.values().stream().filter(vertex -> !transientVertices.contains(vertex)).map(Neo4JVertex::id).collect(Collectors.toList());
            // cypher statement for all vertices not in memory
            Statement statement = filter.isEmpty() ? new Statement("MATCH (n) RETURN n") : new Statement(String.format(Locale.US, "MATCH (n) WHERE not n.%s in {ids} RETURN n", vertexIdFieldName), Values.parameters("ids", filter));
            // create stream from query
            Stream<Vertex> query = vertices(statement);
            // create stream from memory and query result
            Iterator<Vertex> iterator = combine(vertices.values().stream().map(vertex -> (Vertex)vertex), query);
            // it is safe to update loaded flag at this time
            verticesLoaded = true;
            // return iterator
            return iterator;
        }
        // check ids
        if (ids.length > 0) {
            // parameters as a stream (set to remove duplicated ids)
            Set<Object> identifiers = Arrays.stream(ids).map(Neo4JSession::processIdentifier).collect(Collectors.toSet());
            // no need to execute query, only items in memory
            return identifiers.stream()
                .filter(vertices::containsKey)
                .map(id -> (Vertex)vertices.get(id))
                .iterator();
        }
        // no need to execute query, only items in memory
        return vertices.values().stream()
            .map(vertex -> (Vertex)vertex)
            .collect(Collectors.toCollection(LinkedList::new))
            .iterator();
    }

    Stream<Vertex> vertices(Statement statement) {
        Objects.requireNonNull(statement, "statement cannot be null");
        // execute statement (use transaction if available)
        StatementResult result = executeStatement(statement);
        // create stream from result, skip deleted vertices
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(result, Spliterator.NONNULL | Spliterator.IMMUTABLE), false)
            .map(this::loadVertex)
            .filter(vertex -> vertex != null);
    }

    public Iterator<Edge> edges(Object[] ids) {
        Objects.requireNonNull(ids, "ids cannot be null");
        // verify identifiers
        verifyIdentifiers(Edge.class, ids);
        // check we have all edges already loaded
        if (!edgesLoaded) {
            // check ids
            if (ids.length > 0) {
                // parameters as a stream
                Set<Object> identifiers = Arrays.stream(ids).map(Neo4JSession::processIdentifier).collect(Collectors.toSet());
                // filter ids, remove ids already in memory (only ids that might exist on server)
                List<Object> filter = identifiers.stream().filter(id -> !edges.containsKey(id)).collect(Collectors.toList());
                // check we need to execute statement in server
                if (!filter.isEmpty()) {
                    // cypher statement
                    Statement statement = new Statement(String.format(Locale.US, "MATCH (n)-[r]->(m) WHERE r.%s in {ids} RETURN n, r, m", edgeIdFieldName), Values.parameters("ids", filter));
                    // find edges
                    Stream<Edge> query = edges(statement);
                    // combine stream from memory and query result
                    return combine(identifiers.stream().filter(edges::containsKey).map(id -> (Edge)edges.get(id)), query);
                }
                // no need to execute query, only items in memory
                return combine(identifiers.stream().filter(edges::containsKey).map(id -> (Edge)edges.get(id)), Stream.empty());
            }
            // ids in memory (only ids that might exist on server)
            List<Object> filter = edges.values().stream().filter(edge -> !transientEdges.contains(edge)).map(Neo4JEdge::id).collect(Collectors.toList());
            // cypher statement for all edges not in memory
            Statement statement = filter.isEmpty() ? new Statement("MATCH (n)-[r]->(m) RETURN n, r, m") : new Statement(String.format(Locale.US, "MATCH (n)-[r]->(m) WHERE not r.%s in {ids} RETURN n, r, m", edgeIdFieldName), Values.parameters("ids", filter));
            // find edges
            Stream<Edge> query = edges(statement);
            // create stream from memory and query result
            Iterator<Edge> iterator = combine(edges.values().stream().map(edge -> (Edge)edge), query);
            // it is safe to update loaded flag at this time
            edgesLoaded = true;
            // return iterator
            return iterator;
        }
        // check ids
        if (ids.length > 0) {
            // parameters as a stream (set to remove duplicated ids)
            Set<Object> identifiers = Arrays.stream(ids).map(Neo4JSession::processIdentifier).collect(Collectors.toSet());
            // no need to execute query, only items in memory
            return identifiers.stream()
                .filter(edges::containsKey)
                .map(id -> (Edge)edges.get(id))
                .iterator();
        }
        // no need to execute query, only items in memory
        return edges.values().stream()
            .map(edge -> (Edge)edge)
            .collect(Collectors.toCollection(LinkedList::new))
            .iterator();
    }

    Stream<Edge> edges(Statement statement) {
        Objects.requireNonNull(statement, "statement cannot be null");
        // execute statement (use transaction if available)
        StatementResult result = executeStatement(statement);
        // create stream from result, skip deleted edges
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(result, Spliterator.NONNULL | Spliterator.IMMUTABLE), false)
            .map(this::loadEdge)
            .filter(edge -> edge != null);
    }

    private static <T> Iterator<T> combine(Stream<T> collection, Stream<T> query) {
        // create a copy of first stream (state can be modified in the middle of the iteration)
        List<T> copy = collection.collect(Collectors.toCollection(LinkedList::new));
        // iterate query and accumulate to list
        query.forEach(copy::add);
        // return iterator
        return copy.iterator();
    }

    void removeEdge(Neo4JEdge edge, boolean explicit) {
        // edge id
        Object id = edge.id();
        // check edge is transient
        if (transientEdges.contains(edge)) {
            // log information
            if (logger.isDebugEnabled())
                logger.debug("Deleting transient edge: {}", edge);
            // check explicit delete on edge
            if (explicit) {
                // remove references from adjacent vertices
                edge.vertices(Direction.BOTH).forEachRemaining(vertex -> {
                    // remove from vertex
                    ((Neo4JVertex)vertex).removeEdge(edge);
                });
            }
            // remove it from transient set
            transientEdges.remove(edge);
        }
        else {
            // log information
            if (logger.isDebugEnabled())
                logger.debug("Deleting edge: {}", edge);
            // mark edge as deleted (prevent returning edge in query results)
            deletedEdges.add(id);
            // check we need to execute delete statement on edge
            if (explicit)
                edgeDeleteQueue.add(edge);
        }
        // remove edge from map
        edges.remove(id);
    }

    private void registerRelations(Object vertexId, Neo4JEdge edge) {
        // edges associated to vertex
        Set<Neo4JEdge> edges = relations.get(vertexId);
        if (edges == null) {
            // create set
            edges = new HashSet<>();
            // add to relations
            relations.put(vertexId, edges);
        }
        edges.add(edge);
    }

    private static <T> void verifyIdentifiers(Class<T> elementClass, Object... ids) {
        // check length
        if (ids.length > 0) {
            // first element in array
            Object first = ids[0];
            // first element class
            Class<?> firstClass = first.getClass();
            // check it is an element
            if (elementClass.isAssignableFrom(firstClass)) {
                // all ids must be of the same class
                if (!Stream.of(ids).allMatch(id -> elementClass.isAssignableFrom(id.getClass())))
                    throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
            }
            else if (!Stream.of(ids).map(Object::getClass).allMatch(firstClass::equals))
                throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
        }
    }

    private static Object processIdentifier(Object id) {
        // check for Long
        if (id instanceof Long)
            return id;
        // check for numeric types
        if (id instanceof Number)
            return ((Number)id).longValue();
        // check for string
        if (id instanceof String)
            return Long.valueOf((String)id);
        // vertex
        if (id instanceof Vertex)
            return ((Vertex)id).id();
        // edge
        if (id instanceof Edge)
            return ((Edge)id).id();
        // error, TODO get message from resource file
        throw new IllegalArgumentException(String.format("Expected an id that is convertible to Long but received %s", id.getClass()));
    }

    private Vertex loadVertex(Record record) {
        // node
        Node node = record.get(0).asNode();
        // vertex id
        Object vertexId = node.get(vertexIdFieldName).asObject();
        // check vertex has been deleted
        if (!deletedVertices.contains(vertexId)) {
            // create and register vertex
            return registerVertex(new Neo4JVertex(graph, this, propertyIdProvider, vertexIdFieldName, node));
        }
        // skip vertex
        return null;
    }

    private Edge loadEdge(Record record) {
        // relationship
        Relationship relationship = record.get(1).asRelationship();
        // edge id
        Object edgeId = relationship.get(edgeIdFieldName).asObject();
        // check edge has been deleted
        if (!deletedEdges.contains(edgeId)) {
            // check we have record in memory
            Neo4JEdge edge = edges.get(edgeId);
            if (edge == null) {
                // nodes
                Node firstNode = record.get(0).asNode();
                Node secondNode = record.get(2).asNode();
                // node ids
                Object firstNodeId = firstNode.get(vertexIdFieldName).asObject();
                Object secondNodeId = secondNode.get(vertexIdFieldName).asObject();
                // check edge has been deleted (one of the vertices was deleted)
                if (deletedVertices.contains(firstNodeId) || deletedVertices.contains(secondNodeId))
                    return null;
                // check we have first vertex in memory
                Neo4JVertex firstVertex = vertices.get(firstNodeId);
                if (firstVertex == null) {
                    // create vertex
                    firstVertex = new Neo4JVertex(graph, this, propertyIdProvider, vertexIdFieldName, firstNode);
                    // register it
                    registerVertex(firstVertex);
                }
                // check we have second vertex in memory
                Neo4JVertex secondVertex = vertices.get(secondNodeId);
                if (secondVertex == null) {
                    // create vertex
                    secondVertex = new Neo4JVertex(graph, this, propertyIdProvider, vertexIdFieldName, secondNode);
                    // register it
                    registerVertex(secondVertex);
                }
                // find out start and end of the relationship (edge could come in either direction)
                Neo4JVertex out = relationship.startNodeId() == firstNode.id() ? firstVertex : secondVertex;
                Neo4JVertex in = relationship.endNodeId() == firstNode.id() ? firstVertex : secondVertex;
                // create edge
                edge = new Neo4JEdge(graph, this, edgeIdFieldName, out, relationship, in);
                // register with adjacent vertices
                out.addOutEdge(edge);
                in.addInEdge(edge);
                // register edge
                return registerEdge(edge);
            }
            // return edge
            return edge;
        }
        // skip edge
        return null;
    }

    private Vertex registerVertex(Neo4JVertex vertex) {
        // map vertex
        vertices.put(vertex.id(), vertex);
        // return vertex
        return vertex;
    }

    private Edge registerEdge(Neo4JEdge edge) {
        // edge id
        Object id = edge.id();
        // map edge
        edges.put(id, edge);
        // add relations
        edge.vertices(Direction.BOTH).forEachRemaining(vertex -> registerRelations(vertex.id(), edge));
        // return vertex
        return edge;
    }

    void removeVertex(Neo4JVertex vertex) {
        // vertex id
        Object id = vertex.id();
        // check vertex is transient
        if (transientVertices.contains(vertex)) {
            // log information
            if (logger.isDebugEnabled())
                logger.debug("Deleting transient vertex: {}", vertex);
            // remove it from transient set
            transientVertices.remove(vertex);
        }
        else {
            // log information
            if (logger.isDebugEnabled())
                logger.debug("Deleting vertex: {}", vertex);
            // mark vertex as deleted (prevent returning vertex in query results)
            deletedVertices.add(id);
            // add vertex to queue
            vertexDeleteQueue.add(vertex);
        }
        // remove vertex from map
        vertices.remove(id);
    }

    void dirtyVertex(Neo4JVertex vertex) {
        // check element is a transient one
        if (!transientVertices.contains(vertex)) {
            // add vertex to processing queue
            vertexUpdateQueue.add(vertex);
        }
    }

    void dirtyEdge(Neo4JEdge edge) {
        // check element is a transient one
        if (!transientEdges.contains(edge)) {
            // add edge to processing queue
            edgeUpdateQueue.add(edge);
        }
    }

    void flush(Transaction.Status status) {
        // check status
        if (status == Transaction.Status.COMMIT) {
            try {
                // log information
                if (logger.isDebugEnabled())
                    logger.debug("Committing transaction on session [{}]", session.hashCode());
                // delete edges
                deleteEdges();
                // delete vertices
                deleteVertices();
                // create vertices
                createVertices();
                // update vertices
                updateVertices();
                // create edges
                createEdges();
                // update edges
                updateEdges();
                // log information
                if (logger.isDebugEnabled())
                    logger.debug("Successfully committed transaction on session [{}]", session.hashCode());
            }
            catch (ClientException ex) {
                // log error
                if (logger.isErrorEnabled())
                    logger.error("Error committing transaction on session: {}", session.hashCode(), ex);
                // throw original exception
                throw ex;
            }
        }
        else if (logger.isDebugEnabled())
            logger.debug("Rolling back transaction on session: {}", session.hashCode());
        // clean internal caches
        deletedEdges.clear();
        edgeDeleteQueue.clear();
        deletedVertices.clear();
        vertexDeleteQueue.clear();
        transientEdges.clear();
        transientVertices.clear();
        vertexUpdateQueue.clear();
        edgeUpdateQueue.clear();
    }

    private void createVertices() {
        // insert vertices
        for (Neo4JVertex vertex : transientVertices) {
            // create statement
            Statement statement = vertex.insertStatement();
            // log information
            if (logger.isDebugEnabled())
                logger.debug("Executing Cypher statement on transaction [{}]: {}", transaction.hashCode(), statement.toString());
            // execute statement
            transaction.run(statement);
        }
    }

    private void updateVertices() {
        // update vertices
        for (Neo4JVertex vertex : vertexUpdateQueue) {
            // create statement
            Statement statement = vertex.updateStatement();
            // log information
            if (logger.isDebugEnabled())
                logger.debug("Executing Cypher statement on transaction [{}]: {}", transaction.hashCode(), statement.toString());
            // execute statement
            transaction.run(statement);
        }
    }

    private void deleteVertices() {
        // delete vertices
        for (Neo4JVertex vertex : vertexDeleteQueue) {
            // create statement
            Statement statement = vertex.deleteStatement();
            // log information
            if (logger.isDebugEnabled())
                logger.debug("Executing Cypher statement on transaction [{}]: {}", transaction.hashCode(), statement.toString());
            // execute statement
            transaction.run(statement);
        }
    }

    private void createEdges() {
        // insert edges
        for (Neo4JEdge edge : transientEdges) {
            // create statement
            Statement statement = edge.insertStatement();
            // log information
            if (logger.isDebugEnabled())
                logger.debug("Executing Cypher statement on transaction [{}]: {}", transaction.hashCode(), statement.toString());
            // execute statement
            transaction.run(statement);
        }
    }

    private void updateEdges() {
        // update edges
        for (Neo4JEdge edge : edgeUpdateQueue) {
            // create statement
            Statement statement = edge.updateStatement();
            // log information
            if (logger.isDebugEnabled())
                logger.debug("Executing Cypher statement on transaction [{}]: {}", transaction.hashCode(), statement.toString());
            // execute statement
            transaction.run(statement);
        }
    }

    private void deleteEdges() {
        // delete edges
        for (Neo4JEdge edge : edgeDeleteQueue) {
            // create statement
            Statement statement = edge.deleteStatement();
            // log information
            if (logger.isDebugEnabled())
                logger.debug("Executing Cypher statement on transaction [{}]: {}", transaction.hashCode(), statement.toString());
            // execute statement
            transaction.run(statement);
        }
    }

    StatementResult executeStatement(Statement statement) {
        try {
            // check we have a transaction
            if (transaction != null) {
                // log information
                if (logger.isDebugEnabled())
                    logger.debug("Executing Cypher statement on transaction [{}]: {}", transaction.hashCode(), statement.toString());
                // execute on transaction
                return transaction.run(statement);
            }
            // log information
            if (logger.isDebugEnabled())
                logger.debug("Executing Cypher statement on session [{}]: {}", session.hashCode(), statement.toString());
            // execute on session
            return session.run(statement);
        }
        catch (ClientException ex) {
            // log error
            if (logger.isErrorEnabled())
                logger.error("Error executing Cypher statement in session [{}]", session.hashCode(), ex);
            // throw original exception
            throw ex;
        }
    }

    public void close() {
        // log information
        if (logger.isDebugEnabled())
            logger.debug("Closing neo4j session [{}]", session.hashCode());
        // close session
        session.close();
    }

    @Override
    @SuppressWarnings("checkstyle:NoFinalizer")
    protected void finalize() throws Throwable {
        // check session is open
        if (session.isOpen()) {
            // log information
            if (logger.isErrorEnabled())
                logger.error("Finalizing Neo4JSession [{}] without explicit call to close(), the code is leaking sessions!", session.hashCode());
        }
        // base implementation
        super.finalize();
    }
}
