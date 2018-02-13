package com.cms.til.graph.janus;

import java.util.Random;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphTransaction;

public class JanusEdgeUpdator implements Runnable{
	public JanusEdgeUpdator(JanusGraph graph, int parent, int child, int hostid) {
        this.graph = graph;
        this.parent = parent;
        this.child = child;
        this.hostid = hostid;
    }

    private int parent;
    private int child;
    private int hostid;
    private JanusGraph graph;

    public void run() {
    	
    	Thread thread = Thread.currentThread();
    	
        JanusGraphTransaction trxn = graph.newTransaction();
        GraphTraversalSource g = trxn.traversal();
        
        JanusGraphEdge edge = (JanusGraphEdge)g.V().has("msid", parent).outE("hie_child").
        		has("hostid_e", hostid).as("e").
        		inV().has("msid", child).select("e").next();
        Random random = new Random(System.nanoTime());
//        edge.property("updatedAt", random.nextLong());
        edge.property("hrank", random.nextInt());
        trxn.commit();
        System.out.println("Commit completed for " + thread.getName());
    }
}
