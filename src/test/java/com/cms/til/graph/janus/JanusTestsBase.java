package com.cms.til.graph.janus;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.configuration.BaseConfiguration;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

public class JanusTestsBase {

	protected Properties properties;

	protected JanusGraph graph = null;
	
	@Before
	public void createJanusGraphAndUpdateEdge() throws Exception {
		
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
	
	protected void openGraph() throws IOException {
		
		properties = new Properties();
		properties.load(this.getClass().getResourceAsStream("/graph.properties"));
		Assert.assertNotNull(properties);
		
		BaseConfiguration config = new BaseConfiguration();
		
		for(Object property:properties.keySet()){
			System.out.println((String)property + " : " + properties.get(property));
			config.setProperty((String)property, properties.get(property));
		}
		
		graph = JanusGraphFactory.open(config);
	}
	
	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}
}
