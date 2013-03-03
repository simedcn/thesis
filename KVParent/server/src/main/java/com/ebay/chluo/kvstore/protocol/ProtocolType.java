package com.ebay.chluo.kvstore.protocol;

public interface ProtocolType {

	public static final int Heart_Beart = 0;

	public static final int Load_Region = 1;

	public static final int Unload_Region = 2;

	public static final int Split_Region = 3;

	
	
	
	public static final int Set = 10001;

	public static final int Get = 10002;

	public static final int Delete = 10003;

	public static final int Incr = 10004;
	
	public static final int Stat = 10005;
}
