package com.ebay.kvstore.server.data.handler;

import java.io.IOException;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.request.LoadRegionRequest;
import com.ebay.kvstore.protocol.response.LoadRegionResponse;
import com.ebay.kvstore.server.data.DataServerContext;
import com.ebay.kvstore.server.data.storage.IStoreEngine;
import com.ebay.kvstore.structure.Region;

public class LoadRegionRequestHandler extends DataServerHandler<LoadRegionRequest> {

	@Override
	public void handle(DataServerContext context, LoadRegionRequest protocol) {
		IStoreEngine engine = context.getEngine();
		IoSession session = context.getSession();
		Region region = protocol.getRegion();
		IProtocol response = null;
		try {
			engine.loadRegion(region);
			response = new LoadRegionResponse(ProtocolCode.Success, region);
		} catch (IOException e) {
			response = new LoadRegionResponse(ProtocolCode.IOError, region);
		} catch (RuntimeException e) {
			response = new LoadRegionResponse(ProtocolCode.IOError, region);
		}
		session.write(response);
	}
}
