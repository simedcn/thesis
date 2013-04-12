package com.ebay.kvstore.server.master.handler;

import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.exception.KVException;
import com.ebay.kvstore.protocol.request.HeartBeat;
import com.ebay.kvstore.server.master.MasterContext;
import com.ebay.kvstore.server.master.engine.IMasterEngine;
import com.ebay.kvstore.structure.DataServerStruct;

public class HeartBeatHandler extends MasterHandler<HeartBeat> {

	private Logger logger = LoggerFactory.getLogger(HeartBeatHandler.class);

	@Override
	public void handle(MasterContext context, HeartBeat protocol) {
		IMasterEngine engine = context.getEngine();
		DataServerStruct struct = protocol.getStruct();
		IoSession session = context.getSession();
		try {
			engine.syncDataServer(struct, session);
		} catch (KVException e) {
			logger.error("", e);
		}

	}

}
