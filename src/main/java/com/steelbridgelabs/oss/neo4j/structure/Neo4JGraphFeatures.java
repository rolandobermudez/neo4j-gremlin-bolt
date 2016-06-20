package com.steelbridgelabs.oss.neo4j.structure;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.FeatureDescriptor;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public final class Neo4JGraphFeatures implements Graph.Features {

    private static class Neo4JGraphGraphFeatures implements GraphFeatures {

        private static class Neo4JVariableFeatures implements VariableFeatures {

            @Override
            @FeatureDescriptor(name = FEATURE_BOOLEAN_VALUES)
            public boolean supportsBooleanValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BYTE_VALUES)
            public boolean supportsByteValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_DOUBLE_VALUES)
            public boolean supportsDoubleValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_FLOAT_VALUES)
            public boolean supportsFloatValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_INTEGER_VALUES)
            public boolean supportsIntegerValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_LONG_VALUES)
            public boolean supportsLongValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_MAP_VALUES)
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_MIXED_LIST_VALUES)
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BOOLEAN_ARRAY_VALUES)
            public boolean supportsBooleanArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BYTE_ARRAY_VALUES)
            public boolean supportsByteArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_DOUBLE_ARRAY_VALUES)
            public boolean supportsDoubleArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_FLOAT_ARRAY_VALUES)
            public boolean supportsFloatArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_INTEGER_ARRAY_VALUES)
            public boolean supportsIntegerArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_STRING_ARRAY_VALUES)
            public boolean supportsStringArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_LONG_ARRAY_VALUES)
            public boolean supportsLongArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_SERIALIZABLE_VALUES)
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_STRING_VALUES)
            public boolean supportsStringValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_UNIFORM_LIST_VALUES)
            public boolean supportsUniformListValues() {
                return false;
            }
        }

        private VariableFeatures variableFeatures = new Neo4JVariableFeatures();

        @Override
        public VariableFeatures variables() {
            return variableFeatures;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_COMPUTER)
        public boolean supportsComputer() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_THREADED_TRANSACTIONS)
        public boolean supportsThreadedTransactions() {
            return false;
        }
    }

    private static class Neo4JVertexFeatures extends Neo4JElementFeatures implements VertexFeatures {

        private static class Neo4JVertexPropertyFeatures implements VertexPropertyFeatures {

            @Override
            @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_STRING_IDS)
            public boolean supportsStringIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_UUID_IDS)
            public boolean supportsUuidIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_CUSTOM_IDS)
            public boolean supportsCustomIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_ANY_IDS)
            public boolean supportsAnyIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_SERIALIZABLE_VALUES)
            public boolean supportsSerializableValues() {
                return false;
            }

            //            @Override
            //            @FeatureDescriptor(name = FEATURE_INTEGER_VALUES)
            //            public boolean supportsIntegerValues() {
            //                return false;
            //            }
        }

        private final VertexPropertyFeatures vertexPropertyFeatures = new Neo4JVertexPropertyFeatures();

        @Override
        public VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_META_PROPERTIES)
        public boolean supportsMetaProperties() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_MULTI_PROPERTIES)
        public boolean supportsMultiProperties() {
            return false;
        }

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return VertexProperty.Cardinality.single;
        }
    }

    private static class Neo4JEdgeFeatures extends Neo4JElementFeatures implements EdgeFeatures {

        private static class Neo4JEdgePropertyFeatures implements EdgePropertyFeatures {

            @Override
            @FeatureDescriptor(name = FEATURE_SERIALIZABLE_VALUES)
            public boolean supportsSerializableValues() {
                return false;
            }

            //            @Override
            //            @FeatureDescriptor(name = FEATURE_INTEGER_VALUES)
            //            public boolean supportsIntegerValues() {
            //                return false;
            //            }
        }

        private final EdgePropertyFeatures edgePropertyFeatures = new Neo4JEdgePropertyFeatures();

        @Override
        public EdgePropertyFeatures properties() {
            return edgePropertyFeatures;
        }
    }

    private static class Neo4JElementFeatures implements ElementFeatures {

        @Override
        @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_STRING_IDS)
        public boolean supportsStringIds() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_UUID_IDS)
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_ANY_IDS)
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_CUSTOM_IDS)
        public boolean supportsCustomIds() {
            return false;
        }
    }

    private GraphFeatures graphFeatures = new Neo4JGraphGraphFeatures();
    private VertexFeatures vertexFeatures = new Neo4JVertexFeatures();
    private EdgeFeatures edgeFeatures = new Neo4JEdgeFeatures();

    @Override
    public GraphFeatures graph() {
        return graphFeatures;
    }

    @Override
    public VertexFeatures vertex() {
        return vertexFeatures;
    }

    @Override
    public EdgeFeatures edge() {
        return edgeFeatures;
    }

    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }
}