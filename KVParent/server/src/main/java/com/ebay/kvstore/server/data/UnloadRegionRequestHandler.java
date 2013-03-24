package com.ebay.kvstore.server.data;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.request.UnloadRegionRequest;
import com.ebay.kvstore.protocol.response.UnloadRegionResponse;
import com.ebay.kvstore.server.data.storage.IStoreEngine;
import com.ebay.kvstore.structure.Region;

public class UnloadRegionRequestHandler extends DataServerHandler<UnloadRegionRequest> {

	@Override
	public void handle(DataServerContext context, UnloadRegionRequest protocol) {
		IStoreEngine engine = context.getEngine();
		IoSession session = context.getSession();
		int regionId = protocol.getRegionId();
		IProtocol response = null;
		try {
			Region region = engine.unloadRegion(regionId);
			if (region != null) {
				response = new UnloadRegionResponse(ProtocolCode.Success, regionId, region);
			} else {
				response = new UnloadRegionResponse(ProtocolCode.InvalidRegion, regionId, null);
			}
		} catch (Exception e) {
			response = new UnloadRegionResponse(ProtocolCode.IOError, regionId, null);
		}
		session.write(response);
	}

}
