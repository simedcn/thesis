package com.ebay.kvstore.client.async;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.client.IKVClient;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.response.RegionTableResponse;
import com.ebay.kvstore.structure.RegionTable;

/**
 * This should be synchronous
 * 
 * @author luochen
 * 
 */
public class AsyncRegionTableResponseHandler extends AsyncClientHandler<RegionTableResponse> {

	private static Logger logger = LoggerFactory.getLogger(AsyncRegionTableResponseHandler.class);

	@Override
	public void handle(AsyncClientContext context, RegionTableResponse protocol) {
		// TODO Auto-generated method stub
		IKVClient client = context.getClient();
		RegionTable table = protocol.getTable();
		int ret = protocol.getRetCode();
		if (ret != ProtocolCode.Success) {
			logger.error("Fail to get region table from master, reason:",
					ProtocolCode.getMessage(ret));
		}
		client.setRegionTable(table);
		synchronized (client) {
			client.notifyAll();
		}
	}

}
