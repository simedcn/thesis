package com.ebay.kvstore.server.data.handler;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.request.SplitRegionRequest;
import com.ebay.kvstore.protocol.response.SplitRegionResponse;
import com.ebay.kvstore.server.data.DataServerContext;
import com.ebay.kvstore.server.data.storage.IRegionSplitCallback;
import com.ebay.kvstore.server.data.storage.IStoreEngine;
import com.ebay.kvstore.structure.Region;

public class SplitRegionRequestHandler extends DataServerHandler<SplitRegionRequest> {

	@Override
	public void handle(DataServerContext context, SplitRegionRequest protocol) {
		IStoreEngine engine = context.getEngine();
		final IoSession session = context.getSession();
		final int regionId = protocol.getRegionId();
		final int newId = protocol.getNewId();
		engine.splitRegion(regionId, newId, new IRegionSplitCallback() {
			@Override
			public void callback(boolean success, Region oldRegion, Region newRegion) {
				IProtocol response = null;
				if (!success) {
					if (oldRegion == null) {
						response = new SplitRegionResponse(ProtocolCode.Invalid_Region, null, null,
								regionId, newId);
					} else {
						response = new SplitRegionResponse(ProtocolCode.Dataserver_Io_Error, null,
								null, regionId, newId);
					}
				} else {
					response = new SplitRegionResponse(ProtocolCode.Success, oldRegion, newRegion,
							regionId, newId);
				}
				session.write(response);
			}
		});
	}
}
