package com.cms.til.graph.janus;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Test;

public class JanusMultipleEdgesTest extends JanusTestsBase {
	
	private final Integer parent = 20000;
    private final Integer child = 200002;
    private final Integer hostid = 83;
	
	@Test
	public void testMultipleEdgesWithSameId() {
		
		//Sometimes 1st attempt to mutate failing, hence trying 2nd time
//		for(int i=0;i<2;i++){
			concurrentEdgeUpdator();
//		}
		
		System.out.println("Starting evaluating end state!");
		Assert.assertTrue(graph.isOpen());
		
		GraphTraversalSource transaction = graph.traversal();
		
		Long actualEdgeCount = transaction.V().has("msid", parent).outE("hie_child").
				has("hostid_e", hostid).as("e").
				inV().has("msid", child).select("e").count().next();
		
		Long dedupEdgeCount = transaction.V().has("msid", parent).outE("hie_child").
				has("hostid_e", hostid).as("e").
				inV().has("msid", child).select("e").dedup().count().next();
		
		Assert.assertEquals("Duplicate edges found between  (msid: " + parent + ")--hie_child-->(msid: " + child +")",
				dedupEdgeCount, actualEdgeCount);
		
	}
	
	private void concurrentEdgeUpdator() {
		Assert.assertTrue(graph.isOpen());
		
		try {
	        int numThreads = 10;
	        Thread[] threads = new Thread[numThreads];
	        for(int i =0; i < numThreads; i++) {
	            threads[i] = new Thread(new JanusEdgeUpdator(graph, parent, child, hostid));
	        }
	        for(int i = 0; i < numThreads; i++) {
	        	System.out.println("Starting EdgeUpdator thread : " + threads[i].getName());
	            threads[i].start();
	        }
	        for(int i = 0; i < numThreads; i++) {
	        	System.out.println("Waiting for EdgeUpdator thread : " + threads[i].getName());
	            threads[i].join();
	        }
	        
	    } catch (InterruptedException e) {
	    	System.out.println("Exception encountered " + e.getStackTrace());
		}
		
	}
	
}