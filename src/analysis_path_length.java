package kcore;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Arrays;


public class analysis_path_length {
    public static void main(String[] args) {
        String propertyFile = "/public/home/blockchain/master/experiment_kcore_2/janusgraph-hbase-politician.properties";
        JanusGraph graph = JanusGraphFactory.open(propertyFile);
        GraphTraversalSource g = graph.traversal();
        List<Integer> targetValues = Arrays.asList(31);
        int sum = 0;
        List<Vertex> vertices = g.V()
                .has("un_kcore", P.within(targetValues))
                .toList();
        int nodeNum = vertices.size();
        for (Vertex vertex : vertices) {
            int iterateNum = 1;
            while(true){
                long neighborCount = g.V(vertex).repeat(__.both().dedup()).times(iterateNum).count().next();
                if (neighborCount == 0){
                    break;
                }
                iterateNum ++;
            }
            sum += iterateNum;
            System.out.println(vertex);
            System.out.println(iterateNum);
        }
        double avgLength = (double) sum / nodeNum;
        System.out.println("******************");
        System.out.println(avgLength);
        System.out.println(propertyFile);
        System.out.println(sum);
        System.out.println(nodeNum);
        System.out.println("******************");
        graph.close();
    }
}
