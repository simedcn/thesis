package com.ebay.kvstore.client.runner;

import java.lang.reflect.Constructor;
import java.util.concurrent.CountDownLatch;

import org.javasimon.SimonManager;
import org.javasimon.Stopwatch;

import com.ebay.kvstore.client.ClientOption;
import com.ebay.kvstore.client.IKVClient;
import com.ebay.kvstore.client.KVClientFactory;
import com.ebay.kvstore.structure.Address;

public class TestRunnerManager {

	private CountDownLatch latch;
	private IKVClient client;
	private int interval;

	public TestRunnerManager(int interval) {
		ClientOption option = new ClientOption(true, 2000, 10000, Address.parse("localhost:20000"));
		client = KVClientFactory.getClient(option);
		this.interval = interval;
	}

	@SuppressWarnings("rawtypes")
	private void test(Class clazz, int threadCount, int repeat, String key) throws Exception {
		Constructor ctor = clazz.getConstructors()[0];
		latch = new CountDownLatch(threadCount);
		for (int i = 0; i < threadCount; i++) {
			Runnable runnable = (Runnable) ctor.newInstance(client, interval, repeat, latch);
			Thread thread = new Thread(runnable);
			thread.start();
		}
		latch.await();
		Stopwatch watch = SimonManager.getStopwatch(key);
		System.out.println(watch.toString());
	}

	public void testRead(int threadCount, int repeat) throws Exception {
		test(ReadTestRunner.class, threadCount, repeat, "test.read");
	}

	public void testWrite(int threadCount, int repeat) throws Exception {
		test(WriteTestRunner.class, threadCount, repeat, "test.write");
	}

	public void testRandom(int threadCount, int repeat) throws Exception {
		test(RandomTestRunner.class, threadCount, repeat, "test.read");
	}

	public void close() {
		client.close();
	}
}
