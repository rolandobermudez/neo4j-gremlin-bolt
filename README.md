# neo4j-gremlin-bolt

[![Build Status](https://travis-ci.org/SteelBridgeLabs/neo4j-gremlin-bolt.svg?branch=master)](https://travis-ci.org/SteelBridgeLabs/neo4j-gremlin-bolt)

## Graph API

This project allows the use of the [Apache Tinkerpop](http://tinkerpop.apache.org/) Java API with the [neo4j server](http://neo4j.com/).

## Working with Vertices and Edges

### Create a Vertex

Create a new [Vertex](http://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/structure/Vertex.html) in the current `graph` call the [Graph.addVertex()](http://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/structure/Graph.html#addVertex-java.lang.Object...-) method.

```java
  // create a vertex in current graph
  Vertex vertex = graph.addVertex();
```

Create a new [Vertex](http://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/structure/Vertex.html) in the current `graph` with some property values: 

```java
  // create a vertex in current graph with property values
  Vertex vertex = graph.addVertex("name", "John", "age", 50);
```

Create a new [Vertex](http://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/structure/Vertex.html) in the current `graph` with a Label: 

```java
  // create a vertex in current graph with label
  Vertex vertex1 = graph.addVertex("Person");
  // create another vertex in current graph with label
  Vertex vertex2 = graph.addVertex(T.label, "Company");
```
