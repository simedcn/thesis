package com.ebay.kvstore.server.data.storage.task;

import java.lang.reflect.Constructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;
import com.ebay.kvstore.structure.Region;

public class RegionTaskManager {
	private static Logger logger = LoggerFactory.getLogger(RegionTaskManager.class);

	static volatile Boolean lock = false;

	public static boolean flush(IRegionStorage storage, IConfiguration conf,
			IRegionTaskListener listener) {
		try {
			return run(RegionFlusher.class, storage, conf, listener);
		} catch (Exception e) {
			logger.error("Fail to start SplitFlusher Thread", e);
			return false;
		}
	}

	public static boolean merge(IConfiguration conf, IRegionStorage storage1,
			IRegionStorage storage2, IRegionMergeListener listener) {
		try {
			return run(RegionMerger.class, conf, storage1, storage2, listener);
		} catch (Exception e) {
			logger.error("Fail to start RegionMerger Thread", e);
			return false;
		}
	}

	public static Boolean isRunning() {
		return lock;
	}

	public static boolean load(IConfiguration conf, IRegionLoadListener listener, Region region) {
		RegionLoader loader = new RegionLoader(conf, listener, region);
		return loader.load();
	}

	public static boolean split(IRegionStorage storage, IConfiguration conf,
			IRegionTaskListener listener) {
		try {
			return run(RegionSplitter.class, storage, conf, listener);
		} catch (Exception e) {
			logger.error("Fail to start SplitFlusher Thread", e);
			return false;
		}
	}

	private synchronized static <T extends BaseRegionTask> boolean run(Class<T> clazz,
			Object... params) throws Exception {
		if (lock) {
			return false;
		}
		synchronized (lock) {
			if (lock) {
				return false;
			}
			Constructor<T> constructor = ((Constructor<T>[]) clazz.getConstructors())[0];
			BaseRegionTask helper = constructor.newInstance(params);
			Thread t = new Thread(helper);
			lock = true;
			t.start();
			logger.info("New task lauched:" + clazz);
			return true;
		}
	}
}
