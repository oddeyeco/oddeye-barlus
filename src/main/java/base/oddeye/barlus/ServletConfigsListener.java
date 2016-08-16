/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package base.oddeye.barlus;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletContext;
import org.apache.log4j.PropertyConfigurator;

/**
 * Web application lifecycle listener.
 *
 * @author vahan
 */
public class ServletConfigsListener implements ServletContextListener {

    
    @Override
    public void contextInitialized(ServletContextEvent sce) {        
        //System.out.println("ServletContextListener started");
        ServletContext cntxt = sce.getServletContext();
        AppConfiguration.Initbyfile(cntxt);   
        
        // initialize log4j here
        
        String log4jConfigFile = cntxt.getInitParameter("log4j-config-location");
        String fullPath = cntxt.getRealPath("") + log4jConfigFile;
         
        PropertyConfigurator.configure(fullPath);              
        
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
       AppConfiguration.Close();        
    }
}
