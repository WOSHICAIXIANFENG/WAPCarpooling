package com.carpool.util;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * My Application listener
 * @author Xianfeng Cai
 */
public class MyApplicationListener implements ServletContextListener {
	SocketServer ss = null;
	
	@Override
	public void contextInitialized(ServletContextEvent sce) {
		// One this application initialized, I need to start up my Socket server
		ss = SocketServer.getInstance();
        ss.start();
        System.out.println("Samuel Test  MY WEB APPLICATION STARTS...");
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		if (ss != null && ss.hasAnyClientConnection()) {
			// Q: how to close this socket server. (Should I do this ?)
			// ???
			
		}
	}
}
