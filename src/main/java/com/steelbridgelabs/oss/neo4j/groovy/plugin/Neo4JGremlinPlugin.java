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

package com.steelbridgelabs.oss.neo4j.groovy.plugin;

import com.steelbridgelabs.oss.neo4j.structure.Neo4JGraph;
import org.apache.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginInitializationException;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class Neo4JGremlinPlugin extends AbstractGremlinPlugin {

    private static String NAME = "tinkerpop.neo4j.bolt";

    private static final Set<String> IMPORTS = new HashSet<String>() {
        {
            add(IMPORT_SPACE + Neo4JGraph.class.getPackage().getName() + DOT_STAR);
        }
    };

    public String getName() {
        return NAME;
    }

    @Override
    public void afterPluginTo(PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {
        Objects.requireNonNull(pluginAcceptor, "pluginAcceptor cannot be null");
        // process imports
        pluginAcceptor.addImports(IMPORTS);
    }
}
