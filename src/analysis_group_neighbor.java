package kcore;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.*;

public class analysis_group_neighbor {
    public static void main(String[] args) {
        String propertyFile = "/public/home/blockchain/master/experiment_kcore_2/janusgraph-hbase-sport.properties";
        JanusGraph graph = JanusGraphFactory.open(propertyFile);
        GraphTraversalSource g = graph.traversal();
        Object maxCoreValue = g.V().values("un_kcore").max().next();
        Integer maxCore = Integer.parseInt(maxCoreValue.toString());
        Map<Integer, Long> nodeCounts = new HashMap<>();
        Map<Integer, Long> neighborCounts = new HashMap<>();
        Map<Integer, Double> averNeighbor = new HashMap<>();
        for (int i = 0; i <= maxCore; i++) {
            long neighbor = g.V().has("un_kcore", i).both().has("un_kcore", P.neq(i)).dedup().count().next();
            if (neighbor > 0){
                neighborCounts.put(i, neighbor);
            }

            Iterator<Vertex> vertices = g.V().has("un_kcore", i).toList().iterator();
            long node = 0;
            while (vertices.hasNext()) {
                Vertex vertex = vertices.next();
                Object degreeValue = vertex.property("un_kcore_degree").orElse(0);
                node += Long.parseLong(degreeValue.toString());
            }

            if (node > 0){
                nodeCounts.put(i, node);
                double avg = (double) neighbor / node;
                averNeighbor.put(i, avg);
            }
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("sport_group_neighbor.csv"))) {
            writer.write("CoreNumber,NeighborNumber,TotalDegree,Ratio\n");
            for (Map.Entry<Integer, Long> entry : nodeCounts.entrySet()) {
//                writer.write(entry.getKey() + "," + entry.getValue() + "\n");
                int core = entry.getKey();
                long neighbor = neighborCounts.get(core);
                long node = nodeCounts.get(core);
                double avg = averNeighbor.get(core);
                writer.write(core + "," + neighbor + "," + node + "," + avg + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        graph.close();
    }
}
