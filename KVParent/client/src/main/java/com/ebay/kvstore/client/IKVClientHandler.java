package com.ebay.kvstore.client;

import com.ebay.kvstore.client.result.DeleteResult;
import com.ebay.kvstore.client.result.GetResult;
import com.ebay.kvstore.client.result.IncrResult;
import com.ebay.kvstore.client.result.SetResult;
import com.ebay.kvstore.client.result.StatResult;

/**
 * Used for asynchronous call
 * 
 * @author luochen
 * 
 */
public interface IKVClientHandler {

	public void onDelete(DeleteResult result);

	public void onGet(GetResult result);

	public void onIncr(IncrResult result);

	public void onSet(SetResult result);

	public void onStat(StatResult result);

}
