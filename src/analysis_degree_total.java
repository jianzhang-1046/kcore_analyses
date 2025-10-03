package kcore;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.*;

public class analysis_degree_total {
    public static void main(String[] args) {
        String propertyFile = "/public/home/blockchain/master/experiment_kcore/janusgraph-hbase-wikivote.properties";
        JanusGraph graph = JanusGraphFactory.open(propertyFile);
        GraphTraversalSource g = graph.traversal();
        Object maxCoreValue = g.V().values("core").max().next();
        Integer maxCore = Integer.parseInt(maxCoreValue.toString());
        Map<Integer, Long> coreCounts = new HashMap<>();
        g.V().forEachRemaining(vertex -> {
            long degree = g.V(vertex).both().count().next();
            vertex.property("degree", (int) degree);
        });
        for (int i = 1; i <= maxCore; i++) {
            long count = g.V().has("core", i).count().next();
            long degreeSum = g.V().has("core", i).values("degree").sum().tryNext().orElse(0L).longValue();
            //coreCounts.put(i, count);
            //long degree = g.V().properties("degree").next();
            //double averageDegree = count > 0 ? (double) degreeSum / count : 0.0;
            if(count > 0){
                coreCounts.put(i, degreeSum);
            }
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("wikivote_core_total_degree.csv"))) {
                writer.write("CoreNumber,TotalDegree\n");
                for (Map.Entry<Integer, Long> entry : coreCounts.entrySet()) {
                    writer.write(entry.getKey() + "," + entry.getValue() + "\n");
                }
            } catch (IOException e) {
                e.printStackTrace();
        }
        graph.close();
    }
}
