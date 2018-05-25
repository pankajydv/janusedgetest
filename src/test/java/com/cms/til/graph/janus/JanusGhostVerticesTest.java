package com.cms.til.graph.janus;

import java.util.Random;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

public class JanusGhostVerticesTest extends JanusTestsBase {
	
	private final Integer parent = 30000;
	private final Integer child1 = 300002;
	
	GraphTraversalSource g1 = null;
	GraphTraversalSource g2 = null;
	
	@Test
	public void testGhostVerticesCreation () throws Exception {
		g1 = graph.newTransaction().traversal();
		g2 = graph.newTransaction().traversal();
		
		final Vertex v1 = g1.V().has("msid", child1).next();
		final Vertex v2 = g2.V().has("msid", child1).next();
		final Edge e = (Edge)g2.V().has("msid", parent).outE("hie_child").has("hostid_e", 83).as("e").inV().has("msid", child1).select("e").next();
		
		Thread thread1 = new Thread(new Runnable() {
			
			@Override
			public void run() {
				deleteVertex(v1);
				
			}
		}, "vertexdeleter");
		thread1.start();
		thread1.join();
		
		Thread thread2 = new Thread(new Runnable() {
			
			@Override
			public void run() {
				updateEdge(e);
//				updateVertex(v2);
			}
		}, "edgeupdator");
		thread2.start();
		thread2.join();
		
		GraphTraversalSource g3 = graph.newTransaction().traversal();
		Assert.assertTrue("Vertex is not deleted",  g3.V().has("msid", child1).count().next() == 0) ;
		Assert.assertTrue("Deleted vertex is detected as ghost",
				g3.V().has("msid", parent).outE("hie_child").has("hostid_e", 83).inV().hasNot("msid").count().next() == 0  );
		g3.tx().rollback();
	}
	
	private void deleteVertex(Vertex v) {
		g1.V(v.id()).drop().hasNext();
		g1.tx().commit();
	}
	
	private void updateEdge(Edge e) {
		Random random = new Random(System.nanoTime());
		e.property("hrank", random.nextInt());
		g2.tx().commit();
	}
	
	private void updateVertex(Vertex v) {
		Random random = new Random(System.nanoTime());
		v.property("createdAt", random.nextLong());
		g2.tx().commit();
	}
}
