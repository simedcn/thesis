package com.ebay.kvstore.server.master;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.request.HeartBeat;
import com.ebay.kvstore.server.master.helper.IMasterEngine;
import com.ebay.kvstore.structure.DataServerStruct;

public class HeartBeatHandler extends MasterHandler<HeartBeat> {

	@Override
	public void handle(MasterContext context, HeartBeat protocol) {
		IMasterEngine engine = context.getEngine();
		DataServerStruct struct = protocol.getStruct();
		IoSession session = context.getSession();
		engine.syncDataServer(struct, session);

	}

}
