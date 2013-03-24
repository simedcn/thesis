package com.ebay.kvstore.conf;

/**
 * Configuration Key Constants
 * 
 * @author luochen
 * 
 */
public interface IConfigurationKey {

	public static final String Master_Addr = "master.addr";

	public static final String Slave_Addr = "slave.addr";

	public static final String DataServer_Addr = "dataserver.addr";

	public static final String HDFS_Addr = "hdfs.addr";

	public static final String ZooKeeper_Addr = "zookeeper.addr";

	public static final String Storage_Policy = "storage.policy";

	public static final String Master_RedoLog_Max = "master.redolog.max";

	public static final String DataServer_RegoLog_Max = "dataserver.redolog.max";

	public static final String Master_Client_Timeout = "master.client.timeout";

	public static final String DataServer_Master_Timeout = "dataserver.master.timeout";

	public static final String HeartBeat_Interval = "heartbeat.interval";

	public static final String DataServer_Capacity = "dataserver.capacity";

	public static final String Region_Max = "region.max";

	public static final String Region_Block_Size = "region.block.size";

	public static final String Region_Index_Block_Num = "region.index.block.num";

	public static final String DataServer_Connection_Max = "dataserver.connection.max";

	public static final String DataServer_Cache_Max = "dataserver.cache.max";

	public static final String DataServer_Buffer_Max = "dataserver.buffer.max";

	// ////////new added
	public static final String DataServer_Cache_Replacement_Policy = "dataserver.cache.replacement.policy";

	public static final String GC_Check_Interval = "gc.check.interval";

	public static final String Checkpoint_Reserve_Days = "checkpoint.reserve.days";

	public static final String Checkpoint_Interval = "checkpoint.interval";

	public static final String ZooKeeper_Session_Timeout = "zookeeper.session.timeout";

	public static final String DataServer_Reconnect_Interval = "dataserver.reconnect.interval";

	public static final String DataServer_Reconnect_Retry = "dataserver.reconnect.retry";

	public static final String DataServer_Weight = "dataserver.weight";
}
