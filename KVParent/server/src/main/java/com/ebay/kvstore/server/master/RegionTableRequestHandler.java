package com.ebay.kvstore.server.master;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.request.RegionTableRequest;
import com.ebay.kvstore.protocol.response.RegionTableResponse;
import com.ebay.kvstore.server.master.helper.IMasterEngine;
import com.ebay.kvstore.structure.RegionTable;

public class RegionTableRequestHandler extends MasterHandler<RegionTableRequest> {

	@Override
	public void handle(MasterContext context, RegionTableRequest protocol) {
		IMasterEngine engine = context.getEngine();
		IoSession session = context.getSession();
		IProtocol response = null;
		try {
			RegionTable table = engine.getRegionTable();
			response = new RegionTableResponse(ProtocolCode.Success, table);
		} catch (Exception e) {
			response = new RegionTableResponse(ProtocolCode.Master_Error, null);
		}
		session.write(response);
	}

}
