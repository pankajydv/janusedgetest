package com.cms.til.graph.janus;

import java.util.Random;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.RelationTypeIndex;
import org.junit.Assert;

public class JanusGraphCreator {

	private JanusGraph graph = null;
	
	private Integer NUM_L1 = 10;
	private Integer NUM_L2 = 10;
	private Integer MULTIPLIER = 123456;
	
	private Random randomGenerator = new Random(System.nanoTime());
	
	public JanusGraphCreator(JanusGraph graph) {
		this.graph = graph;
	}
	
	public void createJanusGraph() {
		Assert.assertTrue(graph.isOpen());
		
		System.out.println("Staring creating properties and their indexes");
		createPropertiesAndIndexes();
		
		System.out.println("Staring creating Nodes and their edges");
		createNodesAndEdges();
		
		graph.tx().commit();
	}
	
	private void createPropertiesAndIndexes() {
		JanusGraphManagement mgmt = graph.openManagement();
		PropertyKey key = null;
		JanusGraphIndex index = null;
		EdgeLabel label = null;
		
		//Vertex
		if(mgmt.getPropertyKey("msid") == null) {
			key = mgmt.makePropertyKey("msid").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
			index = mgmt.buildIndex("bymsid", Vertex.class).addKey(key).unique().buildCompositeIndex();
			mgmt.setConsistency(index, ConsistencyModifier.LOCK);
		}
		
		if(mgmt.getPropertyKey("type") == null)
			key = mgmt.makePropertyKey("type").dataType(Short.class).cardinality(Cardinality.SINGLE).make();
		if(mgmt.getPropertyKey("createdat") == null)
			key = mgmt.makePropertyKey("createdat").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
		if(mgmt.getPropertyKey("cmstype") == null)
			key = mgmt.makePropertyKey("cmstype").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
		if(mgmt.getPropertyKey("msname") == null)
			key = mgmt.makePropertyKey("msname").dataType(String.class).cardinality(Cardinality.SINGLE).make();
		
		//Edge
		if(mgmt.getEdgeLabel("hie_child") == null)
			label = mgmt.makeEdgeLabel("hie_child").multiplicity(Multiplicity.MULTI).make();

		if(mgmt.getPropertyKey("hostid_e") == null)
			key = mgmt.makePropertyKey("hostid_e").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
		if(mgmt.getPropertyKey("createdat_e") == null)
			key = mgmt.makePropertyKey("createdat_e").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
		if(mgmt.getPropertyKey("cmstype_e") == null)
			key = mgmt.makePropertyKey("cmstype_e").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
		if(mgmt.getPropertyKey("status_e") == null)
			key = mgmt.makePropertyKey("status_e").dataType(Short.class).cardinality(Cardinality.SINGLE).make();
		if(mgmt.getPropertyKey("hname") == null)
			key = mgmt.makePropertyKey("hname").dataType(String.class).cardinality(Cardinality.SINGLE).make();
		if(mgmt.getPropertyKey("hrank") == null)
			key = mgmt.makePropertyKey("hrank").dataType(Float.class).cardinality(Cardinality.SINGLE).make();
		
		RelationType hiechild = mgmt.getRelationType("hie_child");
		if(hiechild ==null || mgmt.getRelationIndex(hiechild, "TestEdgeIndex") == null) {
			RelationTypeIndex edgeIndex = mgmt.buildEdgeIndex(label, "TestEdgeIndex", Direction.BOTH, Order.decr,
				mgmt.getPropertyKey("hostid_e"),
				mgmt.getPropertyKey("status_e"),
				mgmt.getPropertyKey("cmstype_e"),
				mgmt.getPropertyKey("hrank"));
			
			//Following is not allowed, it throws exception:
			//java.lang.IllegalArgumentException: Cannot change consistency of schema element: TestEdgeIndex
			
			//mgmt.setConsistency(edgeIndex, ConsistencyModifier.LOCK);
		}
		
		mgmt.commit();
	}
	
	private void createNodesAndEdges() {
		System.out.println("BEGIN createNodesAndEdges");
		JanusGraphTransaction trxn = graph.newTransaction();
		try {
			//create msid=0;
			Vertex root = createMasterVertex(0, 1);
			
			//create level-1;
			Vertex l1V, l2V;
			for(int i =1; i <= NUM_L1; i++) {
				int l1msid = i*MULTIPLIER;
				l1V = createMasterVertex(l1msid, 2);
				Edge edge1 = createEdge(l1V, root, i);
				edge1.property("cmstype_e", 2);
				
				//create level-2;
				for(int j =1; j <= NUM_L2; j++) {
					int l2msid = l1msid + j;
					l2V = createMasterVertex(l2msid, 1001);
					Edge edge2= createEdge(l2V, l1V, j);
					edge2.property("cmstype_e", 1001);
				}
			}
			System.out.println("END createNodesAndEdges");
		}
		catch (Exception e) {
			System.out.println("Exception while creating vertex and edges. " + e.getStackTrace());
		}
		
		trxn.commit();
	}

	private Vertex createMasterVertex(int msid, int cmstype) {
		Vertex masterV = graph.addVertex();
		masterV.property("msid", msid);
		masterV.property("type", 0);
		masterV.property("createdAt", Math.abs(randomGenerator.nextLong()));
		masterV.property("cmstype", cmstype);
		masterV.property("msname", "Master Vertex " + msid + " Of Type " + cmstype);
		System.out.println("Created MasterVertex: msid: " + msid + "; cmstype: " + cmstype);
		return masterV;
	}


	private Edge createEdge(Vertex childV, Vertex parentV, int rank) {
		Edge edge = parentV.addEdge("hie_child", childV);
		
		edge.property("hostid_e", 83);
		edge.property("createdAt_e", Math.abs(randomGenerator.nextLong()));
		edge.property("status_e", 2);
		edge.property("hname", childV.id() + " Child of Parent " + parentV.id() + ", rank: " + rank);
		edge.property("hrank");
		
		return edge;
	}
}
