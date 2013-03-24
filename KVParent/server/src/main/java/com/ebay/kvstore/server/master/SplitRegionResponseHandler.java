package com.ebay.kvstore.server.master;

import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.Address;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.response.SplitRegionResponse;
import com.ebay.kvstore.server.master.helper.IMasterEngine;
import com.ebay.kvstore.structure.Region;

public class SplitRegionResponseHandler extends MasterHandler<SplitRegionResponse> {
	private static Logger logger = LoggerFactory.getLogger(SplitRegionResponseHandler.class);

	@Override
	public void handle(MasterContext context, SplitRegionResponse protocol) {
		// TODO Auto-generated method stub
		IMasterEngine engine = context.getEngine();
		IoSession session = context.getSession();
		Address addr = Address.parse(session.getRemoteAddress());
		Region oldRegion = protocol.getOldRegion();
		Region newRegion = protocol.getOldRegion();
		int ret = protocol.getRetCode();
		if (ret == ProtocolCode.Success) {
			logger.info(
					"split region from data server {} success, region has been splitted into {} and {}",
					new Object[] { addr, oldRegion, newRegion });
			engine.splitRegion(addr, oldRegion, newRegion);
		} else {
			logger.info("fail to split region from data server {}, reason:", addr,
					ProtocolCode.getMessage(ret));
		}
	}

}
