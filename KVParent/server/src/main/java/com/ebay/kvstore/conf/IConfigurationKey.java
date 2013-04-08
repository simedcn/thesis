package com.ebay.kvstore.conf;

/**
 * Configuration Key Constants
 * 
 * @author luochen
 * 
 */
public interface IConfigurationKey {

	public static final String Master_Addr = "master.addr";

	public static final String Dataserver_Addr = "dataserver.addr";

	public static final String Hdfs_Addr = "hdfs.addr";

	public static final String ZooKeeper_Addr = "zookeeper.addr";

	public static final String Storage_Policy = "storage.policy";

	public static final String Master_Loadbalance_Policy = "master.loadbalance.policy";

	public static final String Master_Unassign_Check_Interval = "master.unassign.check.interval";

	public static final String Master_Unassign_Threshhold = "master.unassign.threshhold";

	public static final String Master_Assign_Check_Interval = "master.assign.check.interval";

	public static final String Master_Split_Check_Interval = "master.split.check.interval";

	public static final String Master_Wait_Dsjoin_Time = "master.wait.dsjoin.time";

	//session timeout needs to be investigate
	public static final String Master_Client_Session_Timeout = "master.client.session.timeout";

	public static final String DataServer_Client_Session_Timeout = "dataserver.client.session.timeout";

	public static final String Dataserver_Master_Connect_Timeout = "dataserver.master.connect.timeout";

	public static final String Dataserver_Master_Session_Timeout = "dataserver.master.session.timeout";

	public static final String Heartbeat_Interval = "heartbeat.interval";

	public static final String Dataserver_Region_Max = "dataserver.region.max";

	public static final String Dataserver_Region_Block_Size = "dataserver.region.block.size";

	public static final String Dataserver_Region_Index_Block_Num = "dataserver.region.index.block.num";
	
	public static final String DataServer_Region_Reserve_Days = "dataserver.region.reserve.days";

	public static final String Dataserver_Connection_Max = "dataserver.connection.max";

	public static final String Dataserver_Cache_Max = "dataserver.cache.max";

	public static final String Dataserver_Buffer_Max = "dataserver.buffer.max";

	// ////////new added
	public static final String Dataserver_Cache_Replacement_Policy = "dataserver.cache.replacement.policy";

	public static final String Master_Gc_Check_Interval = "master.gc.check.interval";

	public static final String Master_Checkpoint_Reserve_Days = "master.checkpoint.reserve.days";

	public static final String Master_Checkpoint_Interval = "master.checkpoint.interval";

	public static final String Zookeeper_Session_Timeout = "zookeeper.session.timeout";

	public static final String Dataserver_Reconnect_Interval = "dataserver.reconnect.interval";

	public static final String Dataserver_Reconnect_Retry_Count = "dataserver.reconnect.retry.count";

	public static final String Dataserver_Weight = "dataserver.weight";

}
