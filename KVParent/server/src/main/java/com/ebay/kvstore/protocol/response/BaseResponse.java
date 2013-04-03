package com.ebay.kvstore.protocol.response;

public abstract class BaseResponse implements IResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected int retCode;

	protected boolean retry;

	public BaseResponse(int retCode) {
		super();
		this.retCode = retCode;
		this.retry = true;
	}

	public BaseResponse(int retCode, boolean retry) {
		this.retCode = retCode;
		this.retry = retry;
	}

	@Override
	public int getRetCode() {
		return retCode;
	}

	public boolean isRetry() {
		return retry;
	}

	public void setRetCode(int retCode) {
		this.retCode = retCode;
	}

	public void setRetry(boolean retry) {
		this.retry = retry;
	}

}
