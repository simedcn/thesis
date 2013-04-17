package com.ebay.kvstore.server.monitor;

import java.io.File;

import org.apache.catalina.core.AprLifecycleListener;
import org.apache.catalina.core.StandardServer;
import org.apache.catalina.startup.Tomcat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.structure.Address;

public class TomcatServer extends BaseWebServer {
	private static Logger logger = LoggerFactory.getLogger(TomcatServer.class);

	protected volatile Tomcat tomcat = null;

	protected Runner runner = null;

	public TomcatServer(Address addr) {
		super(addr);
		this.tomcat = new Tomcat();
	}

	@Override
	public synchronized void start() {
		if (runner != null) {
			throw new IllegalStateException("The tomcat server is already running.");
		}
		logger.info("Starting Tomcat server");
		runner = new Runner();
		runner.start();

	}

	@Override
	public synchronized void stop() throws Exception {
		logger.info("Stopping Tomcat server");
		tomcat.stop();
	}

	private class Runner extends Thread {
		@Override
		public void run() {
			try {
				Tomcat tomcat = new Tomcat();
				tomcat.setHostname(addr.ip);
				tomcat.setPort(addr.port);
				tomcat.setBaseDir(".");
				String base = new File(webappDirLocation).getAbsolutePath();
				tomcat.getHost().setAppBase(base);
				StandardServer server = (StandardServer) tomcat.getServer();
				AprLifecycleListener listener = new AprLifecycleListener();
				server.addLifecycleListener(listener);
				tomcat.getServer().setPort(addr.port + 1);
				tomcat.addWebapp("/", base);
				tomcat.start();
				tomcat.getServer().await();
			} catch (Exception e) {
				logger.error("Fail to start tomcat server", e);
			}
		}
	}

	public static void main(String[] args) {
		try {
			TomcatServer server = new TomcatServer(Address.parse("127.0.0.1:8080"));
			server.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
