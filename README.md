# neo4j-gremlin-bolt

[![Build Status](https://travis-ci.org/SteelBridgeLabs/neo4j-gremlin-bolt.svg?branch=master)](https://travis-ci.org/SteelBridgeLabs/neo4j-gremlin-bolt)

## Graph API

This project allows the use of the [Apache Tinkerpop](http://tinkerpop.apache.org/) Java API with the [neo4j server](http://neo4j.com/).

## Working with Vertices and Edges

### Create a Vertex

To create a Vertex in the current `graph` instance use the [Graph.addVertex()](http://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/structure/Graph.html#addVertex-java.lang.Object...-).

```java
  // create a vertex in current graph
  Vertex vertex = graph.addVertex();
```
