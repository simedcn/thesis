package com.ebay.kvstore.server.data.handler;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.request.MergeRegionRequest;
import com.ebay.kvstore.protocol.response.MergeRegionResponse;
import com.ebay.kvstore.server.data.DataServerContext;
import com.ebay.kvstore.server.data.storage.IRegionMergeCallback;
import com.ebay.kvstore.server.data.storage.IStoreEngine;
import com.ebay.kvstore.structure.Region;

public class MergeRegionRequestHandler extends DataServerHandler<MergeRegionRequest> {
	@Override
	public void handle(DataServerContext context, MergeRegionRequest protocol) {
		IStoreEngine engine = context.getEngine();
		final IoSession session = context.getSession();
		int id1 = protocol.getRegionId1();
		int id2 = protocol.getRegionId2();
		int newId = protocol.getNewRegionId();
		engine.mergeRegion(id1, id2, newId, new IRegionMergeCallback() {
			@Override
			public void callback(boolean success, int region1, int region2, Region region) {
				IProtocol response = null;
				if (!success) {
					if (region == null) {
						response = new MergeRegionResponse(ProtocolCode.Invalid_Region, region1,
								region2, region);
					} else {
						response = new MergeRegionResponse(ProtocolCode.Dataserver_Io_Error,
								region1, region2, region);
					}
				} else {
					response = new MergeRegionResponse(ProtocolCode.Success, region1, region2,
							region);
				}
				session.write(response);
			}
		});
	}

}
