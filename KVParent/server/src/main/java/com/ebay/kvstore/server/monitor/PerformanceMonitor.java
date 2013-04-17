package com.ebay.kvstore.server.monitor;

import org.javasimon.SimonManager;
import org.javasimon.Stopwatch;

import com.ebay.kvstore.structure.Address;

public class PerformanceMonitor implements IPerformanceMonitor {
	private boolean enable;
	private IWebServer webserver;

	PerformanceMonitor(boolean enable, Address addr) {
		this.enable = enable;
		if (enable) {
			webserver = new TomcatServer(addr);
			webserver.start();
		}
	}

	@Override
	public IMonitorObject getMonitorObject(String key) {
		Stopwatch watch = null;
		if (enable) {
			watch = SimonManager.getStopwatch(key);
		}
		return new MonitorObject(watch);
	}

	@Override
	public void dispose() throws Exception {
		if (enable) {
			webserver.stop();
		}

	}

}
