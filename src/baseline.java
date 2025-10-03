package kcore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.graphdb.database.util.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.property;


public class baseline {
    public static void main(String[] args) throws Exception {
        String propertyFile = "/public/home/blockchain/master/experiment_kcore_2/baseline/janusgraph-hbase-politician.properties";
        JanusGraph graph = JanusGraphFactory.open(propertyFile);
        GraphTraversalSource g = graph.traversal();
       basicMethod(g, propertyFile);
    }

    public static void basicMethod(GraphTraversalSource g, String propertyFile) {
        addProperty(g);
        long startTime = System.nanoTime();
        int i = 0;
        while(g.V().has("un_kcore_status", "0").hasNext()){
            i ++;
            System.out.println(i);
            Vertex currentVertex = g.V().has("un_kcore_status", "0")
                    .order().by("un_kcore_degree")
                    .limit(1).next();
            g.V(currentVertex).property("un_kcore", __.values("un_kcore_degree"))
                    .property("un_kcore_status", "1").next();
            Object currentDegree = g.V(currentVertex).values("un_kcore_degree").next();
            g.V(currentVertex).both().has("un_kcore_degree", P.gt(currentDegree))
                    .property("un_kcore_degree", __.values("un_kcore_degree")
                            .math("_ - 1")).iterate();
        }
        g.tx().commit();

        long endTime = System.nanoTime();
        double durationInSeconds = (endTime - startTime) / 1_000_000_000.0;
        System.out.println("***************");
        System.out.println("time: " + String.format("%.1f", durationInSeconds) + " seconds");
        System.out.println(propertyFile);
        System.out.println("***************");
    }


    private static void addProperty(GraphTraversalSource g){
        g.V().forEachRemaining(vertex -> {
            long degree = g.V(vertex).both().count().next();
            vertex.property("un_kcore_degree", (int) degree);
            vertex.property("un_kcore_status", "0");
           vertex.property("group", "0");
        });
        g.tx().commit();
    }

}
