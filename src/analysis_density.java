package kcore;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import java.util.*;

public class analysis_density {
    public static void main(String[] args) {
        String propertyFile = "/public/home/blockchain/master/experiment_kcore/janusgraph-hbase-pokec.properties";
        JanusGraph graph = JanusGraphFactory.open(propertyFile);
        GraphTraversalSource g = graph.traversal();

        int nodeNum = 1632803;
        int edgeNum = 22301964;
        int currentCore = 47;
        List<Map<String, Integer>> records = new ArrayList<>();
        Set<Integer> values = new HashSet<>();

        while (g.V().has("core", currentCore).hasNext()){
            int internalNodeNum = g.V().has("core", currentCore).count().next().intValue();
            System.out.println(internalNodeNum);
            int internalEdgeNum = g.V().has("core", currentCore).bothE().dedup().filter(__.bothV().filter(__.has("status", "1")).count().is(2)).count().next().intValue();
            System.out.println(internalEdgeNum);
            g.V().has("core", currentCore).property("status", "0").iterate();
            nodeNum = nodeNum - internalNodeNum;
            edgeNum = edgeNum - internalEdgeNum;
            Map<String, Integer> record = new HashMap<>();
            record.put("core", currentCore);
            record.put("nodeNum", nodeNum);
            record.put("edgeNum", edgeNum);
            records.add(record);
            System.out.println("newRecords:" + records);
            currentCore --;
        }
        graph.close();
    }
}
