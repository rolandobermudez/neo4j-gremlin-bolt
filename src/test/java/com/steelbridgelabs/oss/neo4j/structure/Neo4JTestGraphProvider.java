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
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 *
 * @author Rogelio J. Baucells
 */
public class Neo4JTestGraphProvider extends AbstractGraphProvider {

    private static final Set<Class> implementations = new HashSet<Class>() {{
        add(Neo4JEdge.class);
        add(Neo4JGraph.class);
        add(Neo4JVertex.class);
    }};

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData graphData) {
        // build configuration
        Configuration configuration = Neo4JGraphConfigurationBuilder.connect("localhost", "neo4j", "123")
            .withElementIdProvider(ElementIdProvider.class)
            .build();
        // create property map from configuration
        Map<String, Object> map = StreamSupport.stream(Spliterators.spliteratorUnknownSize(configuration.getKeys(), Spliterator.NONNULL | Spliterator.IMMUTABLE), false)
            .collect(Collectors.toMap(key -> key, configuration::getProperty));
        // append class name
        map.put(Graph.GRAPH, Neo4JGraph.class.getName());
        // return configuration map
        return map;
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        // check graph instance
        if (graph != null) {
            // close graph instance
            graph.close();
        }
        // connect to server
        try (Driver driver = GraphDatabase.driver(configuration.getString(Neo4JGraphConfigurationBuilder.Neo4JUrlConfigurationKey), AuthTokens.basic(configuration.getString(Neo4JGraphConfigurationBuilder.Neo4JUsernameConfigurationKey), configuration.getString(Neo4JGraphConfigurationBuilder.Neo4JPasswordConfigurationKey)), Config.defaultConfig())) {
            // open session
            try (Session session = driver.session()) {
                // begin transaction
                try (org.neo4j.driver.v1.Transaction transaction = session.beginTransaction()) {
                    // delete everything in database
                    transaction.run(new Statement("MATCH (n) DETACH DELETE n"));
                    // commit
                    transaction.success();
                }
            }
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return implementations;
    }

    @Override
    public void loadGraphData(Graph graph, LoadGraphWith loadGraphWith, Class testClass, String testName) {
        // create indexes for specific test
        if (loadGraphWith != null)
            createIndices((Neo4JGraph)graph, loadGraphWith.value());
        // load graph data
        super.loadGraphData(graph, loadGraphWith, testClass, testName);
    }

    private void createIndices(final Neo4JGraph graph, final LoadGraphWith.GraphData graphData) {
        // process graph data
        switch (graphData) {
            case GRATEFUL:
                // create indexes
                graph.execute("CREATE INDEX ON :artist(" + ElementIdProvider.IdFieldName + ")");
                graph.execute("CREATE INDEX ON :artist(name)");
                graph.execute("CREATE INDEX ON :song(" + ElementIdProvider.IdFieldName + ")");
                graph.execute("CREATE INDEX ON :song(name)");
                graph.execute("CREATE INDEX ON :song(songType)");
                graph.execute("CREATE INDEX ON :song(performances)");
                break;
            case MODERN:
                // create indexes
                graph.execute("CREATE INDEX ON :person(" + ElementIdProvider.IdFieldName + ")");
                graph.execute("CREATE INDEX ON :person(name)");
                graph.execute("CREATE INDEX ON :person(age)");
                graph.execute("CREATE INDEX ON :software(" + ElementIdProvider.IdFieldName + ")");
                graph.execute("CREATE INDEX ON :software(name)");
                graph.execute("CREATE INDEX ON :software(lang)");
                break;
            case CLASSIC:
                // create indexes
                graph.execute("CREATE INDEX ON :vertex(" + ElementIdProvider.IdFieldName + ")");
                graph.execute("CREATE INDEX ON :vertex(name)");
                graph.execute("CREATE INDEX ON :vertex(age)");
                graph.execute("CREATE INDEX ON :vertex(lang)");
                break;
        }
    }
}
