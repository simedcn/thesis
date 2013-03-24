package com.ebay.kvstore.server.data;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.request.SplitRegionRequest;
import com.ebay.kvstore.protocol.response.SplitRegionResponse;
import com.ebay.kvstore.server.data.storage.IRegionSplitCallback;
import com.ebay.kvstore.server.data.storage.IStoreEngine;
import com.ebay.kvstore.structure.Region;

public class SplitRegionRequestHandler extends DataServerHandler<SplitRegionRequest> {

	@Override
	public void handle(DataServerContext context, SplitRegionRequest protocol) {
		// TODO Auto-generated method stub
		IStoreEngine engine = context.getEngine();
		final IoSession session = context.getSession();
		int regionId = protocol.getRegionId();
		int newId = protocol.getNewId();
		engine.splitRegion(regionId, newId, new IRegionSplitCallback() {
			@Override
			public void callback(boolean success, Region oldRegion, Region newRegion) {
				IProtocol response = null;
				if (!success) {
					if (oldRegion == null) {
						response = new SplitRegionResponse(ProtocolCode.InvalidRegion, null, null);
					} else {
						response = new SplitRegionResponse(ProtocolCode.IOError, null, null);
					}
				} else {
					response = new SplitRegionResponse(ProtocolCode.Success, oldRegion, newRegion);
				}
				session.write(response);
			}
		});
	}
}
