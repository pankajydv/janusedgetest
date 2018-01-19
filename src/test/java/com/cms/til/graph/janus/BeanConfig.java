package com.cms.til.graph.janus;

import java.io.InputStream;
import java.util.Properties;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BeanConfig {

	@Bean(name = "properties")
	public Properties setProperties(){

		Properties properties = new Properties();
		
		try {
			InputStream inStream = this.getClass().getResourceAsStream("/graph.properties");
			properties.load(inStream);
		} catch (Exception e) {
			System.out.println("Can't load properties from classpath" + e.getStackTrace());
		}
		
		return properties;
	}
	
}
