package com.ebay.kvstore.client.runner;

import java.util.concurrent.CountDownLatch;

import org.javasimon.SimonManager;

import com.ebay.kvstore.client.IKVClient;
import com.ebay.kvstore.exception.KVException;

public class RandomTestRunner extends BaseTestRunner {

	public RandomTestRunner(IKVClient client, int interval, int count,CountDownLatch latch) {
		super(client, interval, count,latch);
		this.watch = SimonManager.getStopwatch("test.read");
	}

	@Override
	protected void process() throws KVException {
		byte[] key = getRandBytes();
		int op = random.nextInt(4);
		switch (op) {
		case 0:
		case 1:
			client.set(key, key);
			break;
		case 2:
		case 3:
		case 4:
		case 5:
			client.get(key);
			break;
		case 6:
		case 7:
			client.delete(key);
			break;
		case 8:
			client.incr(key, 10, 0);
			break;
		case 9:
			client.stat();
			break;
		}
	}
}
