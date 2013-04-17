package com.ebay.kvstore.server.monitor;

public interface IPerformanceMonitor {

	public static final String Memory_Get_Monitor = "com.ebay.kvstore.data.storage.MemoryStoreEngine.get";
	public static final String Memory_Set_Monitor = "com.ebay.kvstore.data.storage.MemoryStoreEngine.set";
	public static final String Memory_Delete_Monitor = "com.ebay.kvstore.data.storage.MemoryStoreEngine.delete";
	public static final String Memory_Incr_Monitor = "com.ebay.kvstore.data.storage.MemoryStoreEngine.incr";
	public static final String Memory_Stat_Monitor = "com.ebay.kvstore.data.storage.MemoryStoreEngine.stat";
	public static final String Memory_Load_Monitor = "com.ebay.kvstore.data.storage.MemoryStoreEngine.load";
	public static final String Memory_Flush_Monitor = "com.ebay.kvstore.data.storage.MemoryStoreEngine.flush";
	public static final String Memory_Split_Monitor = "com.ebay.kvstore.data.storage.MemoryStoreEngine.split";
	public static final String Memory_Merge_Monitor = "com.ebay.kvstore.data.storage.MemoryStoreEngine.merge";

	public static final String Persistent_Get_Monitor = "com.ebay.kvstore.data.storage.PersistentStoreEngine.get";
	public static final String Persistent_Set_Monitor = "com.ebay.kvstore.data.storage.PersistentStoreEngine.set";
	public static final String Persistent_Delete_Monitor = "com.ebay.kvstore.data.storage.PersistentStoreEngine.delete";
	public static final String Persistent_Incr_Monitor = "com.ebay.kvstore.data.storage.PersistentStoreEngine.incr";
	public static final String Persistent_Stat_Monitor = "com.ebay.kvstore.data.storage.PersistentStoreEngine.stat";
	public static final String Persistent_Load_Monitor = "com.ebay.kvstore.data.storage.task.RegionLoader";
	public static final String Persistent_Flush_Monitor = "com.ebay.kvstore.data.storage.task.RegionFlusher";
	public static final String Persistent_Split_Monitor = "com.ebay.kvstore.data.storage.task.RegionSpliter";
	public static final String Persistent_Merge_Monitor = "com.ebay.kvstore.data.storage.task.RegionMerger";

	public IMonitorObject getMonitorObject(String key);

	public void dispose() throws Exception;
}
