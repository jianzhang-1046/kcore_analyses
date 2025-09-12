package kcore;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.*;

public class analysis_number {
    public static void main(String[] args) {
        String propertyFile = "/public/home/blockchain/master/test/csv_bulkload/lixi/janusgraph-hbase.properties";
        JanusGraph graph = JanusGraphFactory.open(propertyFile);
        GraphTraversalSource g = graph.traversal();
//        Object maxCoreValue = g.V().values("group").max().next();
        // 使用绕过索引优化的方式
        Object maxCoreValue = g.V().has("is_removed", 1)
                .sideEffect(traverser -> {})  // 这可以中断某些优化路径
                .values("group").max().next();
        Integer maxCore = Integer.parseInt(maxCoreValue.toString());
        Map<Integer, Long> coreCounts = new HashMap<>();
        for (int i = 0; i <= maxCore; i++) {
            long count = g.V().has("is_removed", 1).has("group", i).count().next();
            if (count > 0){
                coreCounts.put(i, count);
            }
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("sc_du_dense.csv"))) {
                writer.write("group,NodesNumber\n");
                for (Map.Entry<Integer, Long> entry : coreCounts.entrySet()) {
                    writer.write(entry.getKey() + "," + entry.getValue() + "\n");
                }
            } catch (IOException e) {
                e.printStackTrace();
        }
        graph.close();
    }
}
