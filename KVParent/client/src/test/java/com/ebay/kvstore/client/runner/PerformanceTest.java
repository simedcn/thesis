package com.ebay.kvstore.client.runner;

public class PerformanceTest {

	public static void main(String[] args) {
		TestRunnerManager manager = new TestRunnerManager(1000);
		try {
			manager.testWrite(20, 5);
			manager.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
