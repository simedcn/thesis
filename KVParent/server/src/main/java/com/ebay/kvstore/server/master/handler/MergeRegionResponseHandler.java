package com.ebay.kvstore.server.master.handler;

import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.exception.InvalidDataServerException;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.response.MergeRegionResponse;
import com.ebay.kvstore.server.master.MasterContext;
import com.ebay.kvstore.server.master.engine.IMasterEngine;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

public class MergeRegionResponseHandler extends MasterHandler<MergeRegionResponse> {

	private static Logger logger = LoggerFactory.getLogger(MergeRegionResponseHandler.class);

	@Override
	public void handle(MasterContext context, MergeRegionResponse protocol) {
		IMasterEngine engine = context.getEngine();
		IoSession session = context.getSession();
		Address addr = Address.parse(session.getRemoteAddress());
		int id1 = protocol.getRegionId1();
		int id2 = protocol.getRegionId2();
		Region region = protocol.getRegion();
		int ret = protocol.getRetCode();
		boolean success = (ret == ProtocolCode.Success);
		try {
			if (success) {
				logger.info(
						"merge region from data server {} success, region {} and {} has been merged into {}",
						new Object[] { addr, id1, id2, region });
			} else {
				logger.info("fail to merge region from data server {}, reason:", addr,
						ProtocolCode.getMessage(ret));
			}
			DataServerStruct struct = engine.getDataServerByClient(addr);
			engine.mergeRegion(success, struct, id1, id2, region);
		} catch (InvalidDataServerException e) {
			logger.error("Error occured when merge region", e);
		}
	}

}
