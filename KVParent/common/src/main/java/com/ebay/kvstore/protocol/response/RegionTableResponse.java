package com.ebay.kvstore.protocol.response;

import com.ebay.kvstore.protocol.IProtocolType;
import com.ebay.kvstore.structure.RegionTable;

public class RegionTableResponse extends BaseResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected RegionTable table;

	public RegionTableResponse(int retCode, RegionTable table) {
		super(retCode);
		this.table = table;
	}

	public RegionTable getTable() {
		return table;
	}

	@Override
	public int getType() {
		return IProtocolType.Region_Table_Resp;
	}

	public void setTable(RegionTable table) {
		this.table = table;
	}

	@Override
	public String toString() {
		return "RegionTableResponse [table=" + table + "]";
	}

}
