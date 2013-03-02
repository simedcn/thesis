package com.ebay.chluo.kvstore.protocol;

import java.io.Serializable;

public class SimpleRequest implements Serializable {

	private static final long serialVersionUID = 1L;
	private String name;
	private String value;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public SimpleRequest(String name, String value) {
		super();
		this.name = name;
		this.value = value;
	}

	@Override
	public String toString() {
		return "SimpleRequest [name=" + name + ", value=" + value + "]";
	}

}
