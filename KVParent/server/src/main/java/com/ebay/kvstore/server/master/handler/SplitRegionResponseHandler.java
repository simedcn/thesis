package com.ebay.kvstore.server.master.handler;

import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.exception.InvalidDataServerException;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.response.SplitRegionResponse;
import com.ebay.kvstore.server.master.MasterContext;
import com.ebay.kvstore.server.master.helper.IMasterEngine;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
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
			try {
				logger.info(
						"split region from data server {} success, region has been splitted into {} and {}",
						new Object[] { addr, oldRegion, newRegion });
				DataServerStruct struct = engine.getDataServerByClient(addr);
				engine.splitRegion(struct, oldRegion, newRegion);
			} catch (InvalidDataServerException e) {
				logger.error("Error occured when split region", e);
			}
		} else {
			logger.info("fail to split region from data server {}, reason:", addr,
					ProtocolCode.getMessage(ret));
		}
	}

}
