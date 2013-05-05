package com.ebay.kvstore.server.monitor;

import com.ebay.kvstore.structure.Address;

public class MonitorFactory {

	private static IPerformanceMonitor monitor = null;

	public static IPerformanceMonitor getMonitor() {
		if (monitor == null) {
			throw new IllegalStateException("The monitor has not been initialized yet.");
		}
		return monitor;
	}

	public static void init(boolean enable, Address addr) {
		monitor = new PerformanceMonitor(enable, addr);
	}

}
