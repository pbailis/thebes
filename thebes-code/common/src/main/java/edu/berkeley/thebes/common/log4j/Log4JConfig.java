package edu.berkeley.thebes.common.log4j;

import org.apache.log4j.Appender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.util.Enumeration;

public class Log4JConfig {
    public static void configureLog4J() {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.DEBUG);
        PatternLayout layout = new PatternLayout("%r [%t] %p %c{2} %x - %m%n");
        
        Enumeration<Appender> appenders = Logger.getRootLogger().getAllAppenders();
        while (appenders.hasMoreElements()) {
            appenders.nextElement().setLayout(layout);
        }
    }
}
