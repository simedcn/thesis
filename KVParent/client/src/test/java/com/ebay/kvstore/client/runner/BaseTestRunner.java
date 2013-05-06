package com.ebay.kvstore.client.runner;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.javasimon.Split;
import org.javasimon.Stopwatch;

import com.ebay.kvstore.client.IKVClient;
import com.ebay.kvstore.exception.KVException;

public abstract class BaseTestRunner implements Runnable {

	protected volatile IKVClient client;
	protected Stopwatch watch;
	protected int interval;
	protected int count;
	protected Random random;
	protected CountDownLatch latch;

	public BaseTestRunner(IKVClient client, int interval, int count, CountDownLatch latch) {
		this.client = client;
		this.interval = interval;
		this.count = count;
		this.random = new Random(System.currentTimeMillis());
		this.latch = latch;
	}

	protected abstract void process() throws KVException;

	@Override
	public void run() {
		for (int i = 0; i < count; i++) {
			Split split = null;
			try {
				Thread.sleep(interval);
				split = watch.start();
				process();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if(split!=null){
					split.stop();
				}
			}
		}
		latch.countDown();
	}

	protected byte[] getRandBytes() {
		int length = Math.abs(random.nextInt() % 10000) + 10;
		byte[] bytes = new byte[length];
		random.nextBytes(bytes);
		return bytes;
	}

}
