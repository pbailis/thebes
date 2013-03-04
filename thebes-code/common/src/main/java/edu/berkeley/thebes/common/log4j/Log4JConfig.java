package edu.berkeley.thebes.common.log4j;

import org.apache.log4j.Appender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;

public class Log4JConfig {
    public static void configureLog4J() {
        
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.DEBUG);
        PatternLayout layout = new PatternLayout("%r [%t] %p %c{2} %x - %m%n");

        try {
        	File f = new File("thebes.log");
        	if (f.exists()) {
        		f.delete();
        	}
			Logger.getRootLogger().addAppender(new FileAppender(layout, "thebes.log"));
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        Enumeration<Appender> appenders = Logger.getRootLogger().getAllAppenders();
        
        while (appenders.hasMoreElements()) {
            appenders.nextElement().setLayout(layout);
        }
        
    }
}
