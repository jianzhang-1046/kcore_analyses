package kcore;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.TreeMap;
import java.util.Map;

public class analysis_assortativity {
    public static void main(String[] args) {
        String propertyFile = "/public/home/blockchain/master/experiment_kcore/janusgraph-hbase-grqc.properties";
        JanusGraph graph = JanusGraphFactory.open(propertyFile);
        GraphTraversalSource g = graph.traversal();

        Object maxCoreValue = g.V().values("core").max().next();
        Integer maxCore = Integer.parseInt(maxCoreValue.toString());
        //Map<Integer, Double> assortativityMap = new HashMap<>();
        Map<Integer, Double> assortativityMap = new TreeMap<>();
        for (int i = 1; i <= maxCore; i++) {
            long m = g.V().has("core", i).bothE().count().next(); // Total number of edges for core i
            double sumDjDk = 0.0;
            double sumDj2 = 0.0;
            double sumDk2 = 0.0;

            for (Vertex vertex : g.V().has("core", i).toList()) {
                long degree = g.V(vertex).both().count().next();
                sumDjDk += degree * (m - degree);
                sumDj2 += degree * degree;
            }

            sumDk2 = sumDj2; // Since sumDj2 and sumDk2 are the same in this context

            double assortativity = (sumDjDk / (2 * m)) - ((sumDj2 * sumDk2) / (4 * m * m));
            if (assortativity > 0){
                assortativityMap.put(i, assortativity);
            }    
        }   

        try (BufferedWriter writer = new BufferedWriter(new FileWriter("grqc_core_assortativity.csv"))) {
            writer.write("CoreNumber,Assortativity\n");
            for (Map.Entry<Integer, Double> entry : assortativityMap.entrySet()) {
                writer.write(entry.getKey() + "," + entry.getValue() + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        graph.close();
    }
}

