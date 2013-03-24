package com.ebay.kvstore.conf;

public interface ServerConstants {
	public static final String Proj_Name = "kvstore";

	public static final String ZooKeeper_Base = '/' + Proj_Name;

	public static final String ZooKeeper_Master_Dir = '/' + Proj_Name + "/master";

	public static final String ZooKeeper_Master_Dir_Path = ZooKeeper_Master_Dir + '/';

	public static final String ZooKeeper_Master_Addr = ZooKeeper_Master_Dir_Path + "addr";

	public static final String ZooKeeper_Master_Region_Id = ZooKeeper_Master_Dir_Path + "regionid";

	public static final String ZooKeeper_Data_Dir = '/' + Proj_Name + "/data";

	public static final String ZooKeeper_Data_Dir_Path = ZooKeeper_Data_Dir + '/';

	public static final String DFS_Master_Dir = '/' + Proj_Name + "/master/";

	public static final String DFS_Data_Dir = '/' + Proj_Name + "/data/";

	public static final String Default_Conf_Path = "kvstore.default.properties";

	public static final String Conf_Path = "kvstore.properties";

	public static final String Region_Data_Suffix = ".data";

	public static final String Log_Suffix = ".log";

	public static final String CheckPoint_Suffix = ".ckp";

	public static final int Day = 24 * 3600 * 1000;

	public static final int Hour = 3600 * 1000;

	public static final int Minute = 60 * 1000;
}
