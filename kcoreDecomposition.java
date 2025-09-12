package kcore;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.Transaction;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.util.*;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.*;

public class kcoreDecomposition {
    public static void main(String[] args) {
    batchMethod();
}

    public static void batchMethod(){
        System.out.println("111111111");
        AtomicInteger minDegree = new AtomicInteger((int) addProperty());
        System.out.println("3333333333");
        String propertyFile = "/public/home/blockchain/master/experiment_kcore_2/janusgraph-hbase-sport.properties";
        JanusGraph graph = JanusGraphFactory.open(propertyFile);
        GraphTraversalSource g = graph.traversal();
        boolean kcore_status = g.V().has("un_kcore_status", 0).hasNext();
        while (kcore_status) {
            System.out.println("Reading");
            List<Long> vertices_list = new ArrayList<>();
            for (int curDegree = 0; curDegree <= minDegree.get(); ++curDegree) {
                List<Long> partialList = g.V().has("un_kcore_status", 0)
                        .has("un_kcore_degree", curDegree)
                        .id()
                        .toList()
                        .stream()
                        .map(id -> ((Number) id).longValue())
                        .collect(Collectors.toList());
                vertices_list.addAll(partialList);
                System.out.println(vertices_list);
            }
            if (!vertices_list.isEmpty()) {
                for (long vertex : vertices_list){
                    while (true){
                        try{
                            g.tx().begin();
                            g.V(vertex).property("un_kcore_status", 1).iterate();
                            g.V(vertex).property("un_kcore", minDegree.get()).iterate();
                            g.tx().commit();
                            break;
                        } catch (Exception e) {
                            g.tx().rollback();
                            // exception_deal_1();
                            System.out.println("-----------11111111--------------");
                            System.out.println(e.getMessage());
                        }
                    }
                    // List<Long> adj_list = new ArrayList<>();
                    List<Long> adj_list = g.V(vertex).both()
                            .id()
                            .toList()
                            .stream()
                            .map(id -> ((Number) id).longValue())
                            .collect(Collectors.toList());
                    for (long adj : adj_list) {
                        System.out.println(g.V(adj).valueMap().next());
                        if((int) g.V(adj).values("un_kcore_degree").next() > 0) {
//                            while (true){
                            Integer degree = (int) g.V(adj).values("un_kcore_degree").next();
                            Integer newDegree = degree - 1;
                            try{
                                System.out.println("-----------33333333--------------");
                                g.tx().begin();
                                g.V(adj).property("un_kcore_degree", newDegree).iterate();
                                System.out.println(newDegree);
                                System.out.println("-----------55555555--------------");
                                g.tx().commit();
                                Integer currentDegree = (int) g.V(adj).values("un_kcore_degree").next();
                                if(!currentDegree.equals(newDegree)){
                                    System.out.println("***********************");
                                    System.out.println(currentDegree);
                                    System.out.println(newDegree);
                                    System.out.println("***********************");
                                    throw new Exception("Degrees are not equal!");
                                }
//                                    break;
                            } catch (Exception e) {
//                                    g.tx().rollback();
                                graph.close();
                                exception_deal_2(adj, newDegree);
                                graph = JanusGraphFactory.open(propertyFile);
                                g = graph.traversal();
                                System.out.println("-----------22222222--------------");
                                System.out.println(e.getMessage());
                            }
//                            }
                        }
                    };
                };
            }
            else {
                minDegree.incrementAndGet();
            }
            kcore_status = g.V().has("un_kcore_status", 0).hasNext();
            System.out.println(g.V().has("un_kcore_status", 0).toList());
        }
        g.tx().commit();
        graph.close();
    }

    private static long addProperty(){
        String propertyFile = "/public/home/blockchain/master/experiment_kcore_2/janusgraph-hbase-sport.properties";
        JanusGraph graph = JanusGraphFactory.open(propertyFile);
        final long[] min_degree = {Long.MAX_VALUE};
        GraphTraversalSource g = graph.traversal();
        g.V().forEachRemaining(vertex -> {
            long degree = g.V(vertex).both().count().next();
            if (degree < min_degree[0]){
                min_degree[0] = degree;
            }
            JanusGraphTransaction tx = graph.newTransaction();
            Vertex txVertex = tx.vertices(vertex.id()).next();
            txVertex.property("un_kcore_degree", (int) degree);
            txVertex.property("un_kcore_status", 0);
            System.out.println(txVertex.id());
            tx.commit();
        });
        graph.close();
        return min_degree[0];
    }

    private static void exception_deal_2(long adj, int newdegree){
        String propertyFile = "/public/home/blockchain/master/experiment_kcore_2/janusgraph-hbase-sport.properties";
        JanusGraph graph = JanusGraphFactory.open(propertyFile);
        GraphTraversalSource g = graph.traversal();

        Integer degree = (int) g.V(adj).values("un_kcore_degree").next();
        Integer status = (int) g.V(adj).values("un_kcore_status").next();

        try (JanusGraphTransaction tx = graph.newTransaction()) {
            Vertex originalVertex = tx.traversal().V(adj).next();

            String originalLabel = originalVertex.label();
            Vertex newVertex = tx.addVertex(originalLabel);
            System.out.println("newVertex: " + newVertex);

            originalVertex.properties().forEachRemaining(property -> {
                String key = property.key();
                Object value = property.value();
                if (key.equals("un_kcore_degree")) {
                    newVertex.property(key, newdegree);
                    System.out.println(key + ": " + newdegree);
                } else {
                    newVertex.property(key, value);
                    System.out.println(key + ": " + value);
                }
            });

            originalVertex.vertices(Direction.BOTH).forEachRemaining(neighbor -> {
                newVertex.addEdge("flow", neighbor);
            });

            originalVertex.remove();

            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }

        Map<String, Object> indexRecordPropertyValues = new HashMap<>();

        indexRecordPropertyValues.clear();
        indexRecordPropertyValues.put("un_kcore_degree", degree - 1);
        delete_index(adj, indexRecordPropertyValues, graph, "abIndex");

        graph.tx().commit();
        graph.close();
    }

    private static void delete_index(long adj, Map indexRecordPropertyValues, JanusGraph graph, String index){
        try {
            StaleIndexRecordUtil.forceRemoveVertexFromGraphIndex(
                    adj,
                    indexRecordPropertyValues,
                    graph,
                    index
            );
        } catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
}
