package com.ebay.kvstore.server.master.handler;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.request.DataServerJoinRequest;
import com.ebay.kvstore.protocol.response.DataServerJoinResponse;
import com.ebay.kvstore.server.master.MasterContext;
import com.ebay.kvstore.server.master.helper.IMasterEngine;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;

public class DataServerJoinRequestHandler extends MasterHandler<DataServerJoinRequest> {

	@Override
	public void handle(MasterContext context, DataServerJoinRequest protocol) {
		IMasterEngine engine = context.getEngine();
		IoSession session = context.getSession();
		DataServerStruct struct = protocol.getStruct();
		Address addr = struct.getAddr();
		IProtocol response = null;
		if (engine.containsDataServer(addr)) {
			// should refuse the join
			response = new DataServerJoinResponse(ProtocolCode.Duplicate_Dataserver_Error);
			session.write(response);
		} else {
			response = new DataServerJoinResponse(ProtocolCode.Success);
			session.write(response);
			engine.addDataServer(struct, session);
		}
	}
}
