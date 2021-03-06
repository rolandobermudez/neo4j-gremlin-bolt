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

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactoryClass;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Values;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Rogelio J. Baucells
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@GraphFactoryClass(Neo4JGraphFactory.class)
public class Neo4JGraph implements Graph {

    private class Neo4JTransaction extends AbstractThreadLocalTransaction {

        private final ThreadLocal<org.neo4j.driver.v1.Transaction> transaction = ThreadLocal.withInitial(() -> null);

        public Neo4JTransaction() {
            super(Neo4JGraph.this);
        }

        @Override
        protected void doOpen() {
            // current session
            Neo4JSession session = Neo4JGraph.this.currentSession();
            // open database transaction
            transaction.set(session.beginTransaction());
            // make sure we flush the changes on commit
            addTransactionListener(session::flush);
        }

        @Override
        protected void doCommit() throws TransactionException {
            // current transaction
            org.neo4j.driver.v1.Transaction tx = transaction.get();
            if (tx != null) {
                // indicate everything is ok
                tx.success();
            }
        }

        @Override
        protected void doRollback() throws TransactionException {
            // current transaction
            org.neo4j.driver.v1.Transaction tx = transaction.get();
            if (tx != null) {
                // indicate failure
                tx.failure();
            }
        }

        @Override
        public boolean isOpen() {
            return transaction.get() != null;
        }

        @Override
        protected void doClose() {
            // close base
            super.doClose();
            // current transaction
            org.neo4j.driver.v1.Transaction tx = transaction.get();
            if (tx != null) {
                // close transaction
                tx.close();
                // remove reference
                transaction.remove();
            }
        }
    }

    private final Driver driver;
    private final Neo4JElementIdProvider<?> vertexIdProvider;
    private final Neo4JElementIdProvider<?> edgeIdProvider;
    private final Neo4JElementIdProvider<?> propertyIdProvider;
    private final ThreadLocal<Neo4JSession> session = ThreadLocal.withInitial(() -> null);
    private final Neo4JTransaction transaction = new Neo4JTransaction();

    public Neo4JGraph(Driver driver, Neo4JElementIdProvider<?> vertexIdProvider, Neo4JElementIdProvider<?> edgeIdProvider, Neo4JElementIdProvider<?> propertyIdProvider) {
        Objects.requireNonNull(driver, "driver cannot be null");
        Objects.requireNonNull(vertexIdProvider, "vertexIdProvider cannot be null");
        Objects.requireNonNull(edgeIdProvider, "edgeIdProvider cannot be null");
        Objects.requireNonNull(propertyIdProvider, "propertyIdProvider cannot be null");
        // store driver instance
        this.driver = driver;
        // store providers
        this.vertexIdProvider = vertexIdProvider;
        this.edgeIdProvider = edgeIdProvider;
        this.propertyIdProvider = propertyIdProvider;
    }

    private Neo4JSession currentSession() {
        // get current session
        Neo4JSession session = this.session.get();
        if (session == null) {
            // create new session
            session = new Neo4JSession(this, driver.session(), vertexIdProvider, edgeIdProvider, propertyIdProvider);
            // attach it to current thread
            this.session.set(session);
        }
        return session;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        // get current session
        Neo4JSession session = currentSession();
        // add vertex
        return session.addVertex(keyValues);
    }

    public StatementResult execute(Statement statement) {
        Objects.requireNonNull(statement, "statement cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // execute statement
        return session.executeStatement(statement);
    }

    public StatementResult execute(String statement) {
        Objects.requireNonNull(statement, "statement cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // execute statement
        return session.executeStatement(new Statement(statement));
    }

    public StatementResult execute(String statement, Map<String, Object> parameters) {
        Objects.requireNonNull(statement, "statement cannot be null");
        Objects.requireNonNull(parameters, "parameters cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // execute statement
        return session.executeStatement(new Statement(statement, Values.value(parameters)));
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> implementation) throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Iterator<Vertex> vertices(Object... ids) {
        // get current session
        Neo4JSession session = currentSession();
        // find vertices
        return session.vertices(ids);
    }

    public Iterator<Vertex> vertices(Statement statement) {
        Objects.requireNonNull(statement, "statement cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // find vertices
        return session.vertices(statement)
            .collect(Collectors.toCollection(LinkedList::new))
            .iterator();
    }

    public Iterator<Vertex> vertices(String statement) {
        Objects.requireNonNull(statement, "statement cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // find vertices
        return session.vertices(new Statement(statement))
            .collect(Collectors.toCollection(LinkedList::new))
            .iterator();
    }

    public Iterator<Vertex> vertices(String statement, Map<String, Object> parameters) {
        Objects.requireNonNull(statement, "statement cannot be null");
        Objects.requireNonNull(parameters, "parameters cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // find vertices
        return session.vertices(new Statement(statement, parameters))
            .collect(Collectors.toCollection(LinkedList::new))
            .iterator();
    }

    @Override
    public Iterator<Edge> edges(Object... ids) {
        // get current session
        Neo4JSession session = currentSession();
        // find edges
        return session.edges(ids);
    }

    public Iterator<Edge> edges(Statement statement) {
        Objects.requireNonNull(statement, "statement cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // find vertices
        return session.edges(statement)
            .collect(Collectors.toCollection(LinkedList::new))
            .iterator();
    }

    public Iterator<Edge> edges(String statement) {
        Objects.requireNonNull(statement, "statement cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // find vertices
        return session.edges(new Statement(statement))
            .collect(Collectors.toCollection(LinkedList::new))
            .iterator();
    }

    public Iterator<Edge> edges(String statement, Map<String, Object> parameters) {
        Objects.requireNonNull(statement, "statement cannot be null");
        Objects.requireNonNull(parameters, "parameters cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // find vertices
        return session.edges(new Statement(statement, parameters))
            .collect(Collectors.toCollection(LinkedList::new))
            .iterator();
    }

    @Override
    public Transaction tx() {
        // open transaction if needed
        if (!transaction.isOpen())
            transaction.open();
        // return transaction
        return transaction;
    }

    @Override
    public Variables variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }

    @Override
    public Configuration configuration() {
        return null;
    }

    @Override
    public void close() {
        // get current session
        Neo4JSession session = this.session.get();
        if (session != null) {
            // close session
            session.close();
            // remove session
            this.session.remove();
        }
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "");
    }

    @Override
    public Features features() {
        return new Neo4JGraphFeatures();
    }
}
