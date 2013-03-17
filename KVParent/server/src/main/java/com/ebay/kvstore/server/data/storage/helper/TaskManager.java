package com.ebay.kvstore.server.data.storage.helper;

import java.lang.reflect.Constructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.Address;
import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;
import com.ebay.kvstore.structure.Region;

/**
 * Used for provide locking for flusher. Our system only permit a single
 * flusher/splitter thread run at a single time.
 * 
 * @author luochen
 * 
 */
public class TaskManager {
	private static Logger logger = LoggerFactory.getLogger(TaskManager.class);

	static volatile Boolean lock = false;

	public static Boolean isRunning() {
		return lock;
	}

	public static boolean load(IConfiguration conf, IRegionLoadListener listener, Region region,
			Address addr, boolean async) {
		{
			if (async) {
				try {
					return run(RegionLoader.class, conf, listener, region, addr);
				} catch (Exception e) {
					logger.error("Fail to start RegionLoader Thread", e);
					return false;
				}
			} else {
				RegionLoader loader = new RegionLoader(conf, listener, region, addr);
				loader.run();
				return true;
			}
		}

	}

	public static boolean flush(IRegionStorage storage, IConfiguration conf,
			IRegionListener listener) {
		try {
			return run(RegionFlusher.class, storage, conf, listener);
		} catch (Exception e) {
			logger.error("Fail to start SplitFlusher Thread", e);
			return false;
		}
	}

	public static boolean split(IRegionStorage storage, IConfiguration conf,
			IRegionListener listener) {
		try {
			return run(RegionSplitter.class, storage, conf, listener);
		} catch (Exception e) {
			logger.error("Fail to start SplitFlusher Thread", e);
			return false;
		}
	}

	private synchronized static <T extends BaseHelper> boolean run(Class<T> clazz, Object... params)
			throws Exception {
		if (lock) {
			return false;
		}
		synchronized (lock) {
			if (lock) {
				return false;
			}
			Constructor<T> constructor = ((Constructor<T>[]) clazz.getConstructors())[0];
			BaseHelper helper = constructor.newInstance(params);
			Thread t = new Thread(helper);
			lock = true;
			t.start();
			logger.info("New task lauched:" + clazz);
			return true;
		}
	}
}
