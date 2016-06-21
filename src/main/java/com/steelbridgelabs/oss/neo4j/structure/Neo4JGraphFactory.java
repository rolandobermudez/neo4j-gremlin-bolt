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
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

/**
 *
 * @author Rogelio J. Baucells
 */
public class Neo4JGraphFactory {

    public static Graph open(Configuration configuration) {
        if (null == configuration)
            throw Graph.Exceptions.argumentCanNotBeNull("configuration");
        try {
            // create driver instance
            Driver driver = GraphDatabase.driver(configuration.getString(Neo4JGraphConfigurationBuilder.Neo4JUrlConfigurationKey), AuthTokens.basic(configuration.getString(Neo4JGraphConfigurationBuilder.Neo4JUsernameConfigurationKey), configuration.getString(Neo4JGraphConfigurationBuilder.Neo4JPasswordConfigurationKey)), Config.defaultConfig());
            // create providers
            Neo4JElementIdProvider<?> vertexIdProvider = loadProvider(configuration.getString(Neo4JGraphConfigurationBuilder.Neo4JVertexIdProviderClassNameConfigurationKey));
            Neo4JElementIdProvider<?> edgeIdProvider = loadProvider(configuration.getString(Neo4JGraphConfigurationBuilder.Neo4JEdgeIdProviderClassNameConfigurationKey));
            Neo4JElementIdProvider<?> propertyIdProvider = loadProvider(configuration.getString(Neo4JGraphConfigurationBuilder.Neo4JPropertyIdProviderClassNameConfigurationKey));
            // create graph instance
            return new Neo4JGraph(driver, vertexIdProvider, edgeIdProvider, propertyIdProvider);
        }
        catch (Throwable ex) {
            // throw runtime exception
            throw new RuntimeException("Error creating Graph instance from configuration", ex);
        }
    }

    private static Neo4JElementIdProvider<?> loadProvider(String className) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        // check class name
        if (className != null) {
            // load class
            Class<?> type = Class.forName(className);
            // create instance
            return (Neo4JElementIdProvider<?>)type.newInstance();
        }
        return null;
    }
}
