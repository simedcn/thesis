package com.ebay.kvstore.structure;

import java.io.Serializable;
import java.util.List;

public class DataServerStruct implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String ip;

	private int port;

	private int weight;

	private List<Region> regions;

	public DataServerStruct(String ip, int port, int weight, List<Region> regions) {
		super();
		this.ip = ip;
		this.port = port;
		this.weight = weight;
		this.regions = regions;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public List<Region> getRegions() {
		return regions;
	}

	public void setRegions(List<Region> regions) {
		this.regions = regions;
	}

}
