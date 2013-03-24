package com.ebay.kvstore.server.master;

import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.Address;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.response.LoadRegionResponse;
import com.ebay.kvstore.server.master.helper.IMasterEngine;
import com.ebay.kvstore.structure.Region;

public class LoadRegionResponseHandler extends MasterHandler<LoadRegionResponse> {
	private static Logger logger = LoggerFactory.getLogger(LoadRegionResponseHandler.class);

	@Override
	public void handle(MasterContext context, LoadRegionResponse protocol) {
		IMasterEngine engine = context.getEngine();
		IoSession session = context.getSession();
		Address addr = Address.parse(session.getRemoteAddress());
		Region region = protocol.getRegion();
		int ret = protocol.getRetCode();
		if (ret == ProtocolCode.Success) {
			logger.info("assign region {} to data server {} success.", region.getRegionId(), addr);
			engine.loadRegion(addr, region);
		} else {
			logger.info("fail to assign region to data server {}, reason:", addr,
					ProtocolCode.getMessage(ret));
		}
	}
}
