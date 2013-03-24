package com.ebay.kvstore.server.master;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.request.StatRequest;
import com.ebay.kvstore.protocol.response.StatResponse;
import com.ebay.kvstore.server.master.helper.IMasterEngine;
import com.ebay.kvstore.structure.DataServerStruct;

public class StatRequestHandler extends MasterHandler<StatRequest> {

	@Override
	public void handle(MasterContext context, StatRequest protocol) {
		IMasterEngine engine = context.getEngine();
		IoSession session = context.getSession();
		IProtocol response = null;
		DataServerStruct[] structs = engine.getAllDataServers();
		response = new StatResponse(ProtocolCode.Success, structs);
		session.write(response);
	}

}
