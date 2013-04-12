package com.ebay.kvstore.server.data;

import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.request.HeartBeat;
import com.ebay.kvstore.server.data.storage.IStoreEngine;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

/**
 * Used for sending heat beat periodically.
 * 
 * @author luochen
 * 
 */
public class HeartBeater {
	// in milliseconds
	private static Logger logger = LoggerFactory.getLogger(HeartBeater.class);

	private int interval;

	private IoSession session;

	private IStoreEngine engine;

	private IConfiguration conf;

	private Address addr;

	private Thread runner;

	private int weight;

	public HeartBeater(IConfiguration conf, IStoreEngine engine, IoSession session) {
		this.conf = conf;
		this.engine = engine;
		this.session = session;
		this.addr = Address.parse(this.conf.get(IConfigurationKey.Dataserver_Addr));
		this.weight = this.conf.getInt(IConfigurationKey.Dataserver_Weight);
		this.interval = this.conf.getInt(IConfigurationKey.Heartbeat_Interval);
	}

	public void start() {
		runner = new Thread(new Runner());
		runner.start();
	}

	@SuppressWarnings("deprecation")
	public void stop() {
		if (runner != null) {
			runner.stop();
			runner = null;
		}
	}

	private void heatbeat() {
		engine.stat();
		DataServerStruct struct = new DataServerStruct(addr, weight);
		Region[] regions = engine.getAllRegions();
		struct.addRegion(regions);
		IProtocol heatbeat = new HeartBeat(struct);
		session.write(heatbeat);
	}

	private class Runner implements Runnable {
		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(interval);
					heatbeat();
				} catch (Exception e) {
					logger.error("Error orrcued during heartbeat", e);
				}
			}
		}
	}

}
