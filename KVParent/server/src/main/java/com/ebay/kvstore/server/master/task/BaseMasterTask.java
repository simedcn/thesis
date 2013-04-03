package com.ebay.kvstore.server.master.task;

import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.exception.KVException;
import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.server.master.helper.IMasterEngine;
import com.ebay.kvstore.structure.Address;

public abstract class BaseMasterTask implements IMasterTask {

	protected Logger logger = LoggerFactory.getLogger(BaseMasterTask.class);

	protected int interval;

	protected IConfiguration conf;

	protected Thread runner;

	protected IMasterEngine engine;

	protected int wait;

	public BaseMasterTask(IConfiguration conf, IMasterEngine engine) {
		this.conf = conf;
		this.engine = engine;
		this.wait = this.conf.getInt(IConfigurationKey.Master_Wait_DsJoin_Time);
		this.interval = 10000;
	}

	@Override
	public String getName() {
		return this.getClass().getSimpleName();
	}

	@Override
	public void start() {
		runner = new Thread(new Runner());
		runner.start();
	}

	@Override
	@SuppressWarnings("deprecation")
	public void stop() {
		if (runner != null) {
			runner.stop();
			runner = null;
		}
	}

	protected abstract void process();

	protected void sendRequest(Address address, IProtocol request) {
		try {
			IoSession session = engine.getDataServerConnection(address);
			session.write(request);
			logger.info("Send Request {} to {} to load Region {}", request, address);
		} catch (KVException e) {
			logger.error("Fail to send " + request + " to " + address, e);
		}
	}

	protected final class Runner implements Runnable {
		@Override
		public void run() {
			try {
				int waitTime = wait - interval;
				if (waitTime > 0) {
					Thread.sleep(waitTime);
				}
			} catch (InterruptedException e1) {
			}
			logger.info("{} started", getName());
			while (true) {
				try {
					Thread.sleep(interval);
					process();
				} catch (Exception e) {
					logger.error("Error orrcued during " + getName(), e);
				}
			}
		}
	}
}
