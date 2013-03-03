package com.ebay.chluo.kvstore.protocol.response;



public abstract class BaseResponse implements IResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected int retCode;

	public BaseResponse(int retCode) {
		super();
		this.retCode = retCode;
	}

	public int getRetCode() {
		return retCode;
	}

	public void setRetCode(int retCode) {
		this.retCode = retCode;
	}

}