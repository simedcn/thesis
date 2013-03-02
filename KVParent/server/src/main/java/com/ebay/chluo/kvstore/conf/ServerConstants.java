package com.ebay.chluo.kvstore.conf;

public class ServerConstants {
	private static final String Proj_Name = "kvstore";

	public static final String ZooKeeper_Base = '/' + Proj_Name;

	public static final String ZooKeeper_Master_Dir = '/' + Proj_Name + "/master";

	public static final String ZooKeeper_Master_Dir_Path = ZooKeeper_Master_Dir + '/';

	public static final String ZooKeeper_Data_Dir = '/' + Proj_Name + "/data";
	
	public static final String ZooKeeper_Data_Dir_Path = ZooKeeper_Data_Dir + '/';

}
