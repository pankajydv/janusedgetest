package com.cms.til.graph.janus;

import java.util.Properties;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { BeanConfig.class })
public class JanusMultipleEdgesTest {
	
	private final Integer parent = 246912;
    private final Integer child = 246913;
    private final Integer hostid = 83;
	
	@Autowired
	private Properties properties;
	
	private JanusGraph graph = null;
	
	@Before
	public void createJanusGraphAndUpdateEdge() {
		
		openGraph();
		JanusGraphCreator creator = new JanusGraphCreator(graph);
		creator.createJanusGraph();
		
	}
	
	@After
	public void shutdown() {
		System.out.println("Shutting down the graph!");
		
		if(graph != null && graph.isOpen()) {
			graph.traversal().V().drop();
			graph.close();
		}
	}
	
	@Test
	public void testMultipleEdgesWithSameId() {
		
		//Sometimes 1st attempt to mutate failing, hence trying 2nd time
		for(int i=0;i<2;i++){
			concurrentEdgeUpdator();
		}
		
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
	
	private void openGraph(){
		
		Assert.assertNotNull(properties);
		
		BaseConfiguration config = new BaseConfiguration();
		
		for(Object property:properties.keySet()){
			System.out.println((String)property + " : " + properties.get(property));
			config.setProperty((String)property, properties.get(property));
		}
		
		graph = JanusGraphFactory.open(config);
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