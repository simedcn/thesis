package com.ebay.kvstore.server.master.handler;

import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.exception.KVException;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.response.UnloadRegionResponse;
import com.ebay.kvstore.server.master.MasterContext;
import com.ebay.kvstore.server.master.engine.IMasterEngine;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

public class UnloadRegionResponseHandler extends MasterHandler<UnloadRegionResponse> {

	private static Logger logger = LoggerFactory.getLogger(UnloadRegionResponseHandler.class);

	@Override
	public void handle(MasterContext context, UnloadRegionResponse protocol) {
		IMasterEngine engine = context.getEngine();
		IoSession session = context.getSession();
		Address addr = Address.parse(session.getRemoteAddress());
		Region region = protocol.getRegion();
		int regionId = protocol.getRegionId();
		int ret = protocol.getRetCode();
		boolean success = ret == ProtocolCode.Success;
		try {
			if (success) {
				logger.info("unassign region {} from data server {} success.",
						region.getRegionId(), addr);
			} else {
				logger.info("fail to assign region to data server {}, reason:", addr,
						ProtocolCode.getMessage(ret));
			}
			DataServerStruct struct = engine.getDataServerByClient(addr);
			engine.unloadRegoin(success, struct, region, regionId);
		} catch (KVException e) {
			logger.error("Error occured when unload region", e);
		}
	}

}
