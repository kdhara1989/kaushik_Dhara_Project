package com.cs;

import java.util.Properties;
import org.apache.log4j.PropertyConfigurator;
import com.cs.service.LogService;

/**
 * @author Kaushik
 *
 */
public class Application {
	
	public static void main(String[] args) {
		
		setLogPropertries();
		LogService app = new LogService();
		app.analyzeLogFile();
	}
	
	private static void setLogPropertries() {
		Properties prop = new Properties(); 
		prop.setProperty("log4j.rootLogger", "INFO,stdout");
		prop.setProperty("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
		prop.setProperty("log4j.appender.stdout.Target", "System.out"); 
		prop.setProperty("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout"); 
		prop.setProperty("log4j.appender.stdout.layout.ConversionPattern", "%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1} - %m%n"); 
		PropertyConfigurator.configure(prop);
	}
	
}
