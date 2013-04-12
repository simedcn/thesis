package com.ebay.kvstore.server.master.engine;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

public class MasterEngineListenerManager implements IMasterEngineListener {

	private Set<IMasterEngineListener> listeners;

	private static Logger logger = LoggerFactory.getLogger(MasterEngineListenerManager.class);

	public MasterEngineListenerManager() {
		listeners = new HashSet<>();
	}

	@Override
	public void onDataServerUnload(DataServerStruct struct) {
		for (IMasterEngineListener listener : listeners) {
			try {
				listener.onDataServerUnload(struct);
			} catch (Exception e) {
				logger.error("Error occured when calling onDataServerUnload on listener", e);
			}
		}
	}

	@Override
	public void onRegionLoad(DataServerStruct struct, Region region) {
		for (IMasterEngineListener listener : listeners) {
			try {
				listener.onRegionLoad(struct, region);
			} catch (Exception e) {
				logger.error("Error occured when calling onRegionLoad on listener", e);
			}
		}
	}

	@Override
	public void onRegionSplit(int oldRegion, int newRegion) {
		for (IMasterEngineListener listener : listeners) {
			try {
				listener.onRegionSplit(oldRegion, newRegion);
			} catch (Exception e) {
				logger.error("Error occured when calling onRegionSplit on listener", e);
			}
		}
	}

	@Override
	public void onRegionUnload(DataServerStruct struct, int region) {
		for (IMasterEngineListener listener : listeners) {
			try {
				listener.onRegionUnload(struct, region);
			} catch (Exception e) {
				logger.error("Error occured when calling onRegionUnload on listener", e);
			}
		}
	}

	public void registerListener(IMasterEngineListener listener) {
		listeners.add(listener);
	}

	public void unregisterAll() {
		listeners.clear();
	}

	public void unregisterListener(IMasterEngineListener listener) {
		listeners.add(listener);
	}

	@Override
	public void onRegionMerge(DataServerStruct struct, int regionId1, int regionId2) {
		for (IMasterEngineListener listener : listeners) {
			try {
				listener.onRegionMerge(struct, regionId1, regionId2);
			} catch (Exception e) {
				logger.error("Error occured when calling onRegionUnload on listener", e);
			}
		}
	}

}
