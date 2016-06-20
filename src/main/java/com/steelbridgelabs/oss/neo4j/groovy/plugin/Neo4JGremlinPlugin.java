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
