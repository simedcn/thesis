package com.ebay.kvstore.structure;

import java.io.Serializable;
import java.lang.management.ManagementFactory;

import com.sun.management.OperatingSystemMXBean;

public class SystemInfo implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private long memoryTotal = 0;

	private long memoryFree = 0;

	private double cpuUsage = 0;

	public SystemInfo() {
		OperatingSystemMXBean osmb = (OperatingSystemMXBean) ManagementFactory
				.getOperatingSystemMXBean();
		memoryTotal = osmb.getTotalPhysicalMemorySize();
		memoryFree = osmb.getFreePhysicalMemorySize();
		cpuUsage = osmb.getSystemCpuLoad();
	}

	public SystemInfo(long memoryTotal, long memoryFree, double cpuUsage) {
		super();
		this.memoryTotal = memoryTotal;
		this.memoryFree = memoryFree;
		this.cpuUsage = cpuUsage;
	}

	public double getCpuUsage() {
		return cpuUsage;
	}

	public long getMemoryFree() {
		return memoryFree;
	}

	public long getMemoryTotal() {
		return memoryTotal;
	}

	@Override
	public String toString() {
		return "SystemInfo [memoryTotal=" + memoryTotal + ", memoryFree=" + memoryFree
				+ ", cpuUsage=" + cpuUsage + "]";
	}

}
