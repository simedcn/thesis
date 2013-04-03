package com.ebay.kvstore.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.client.async.result.DeleteResult;
import com.ebay.kvstore.client.async.result.GetResult;
import com.ebay.kvstore.client.async.result.IncrResult;
import com.ebay.kvstore.client.async.result.SetResult;
import com.ebay.kvstore.client.async.result.StatResult;
import com.ebay.kvstore.exception.KVException;
import com.ebay.kvstore.structure.Address;

public class TestAsyncClient extends BaseClientTest {

	protected ClientOption option;

	public TestAsyncClient() {
		option = new ClientOption(false, 2000, new Address("127.0.0.1", 20000));
		initClient(option);
		client.setHandler(new Handler());
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testOperation() {
		try {
			for (byte i = 0; i < 100; i++) {
				client.set(new byte[] { i }, new byte[] { i });
			}
			Thread.sleep(1000);
			for (byte i = 0; i < 100; i += 2) {
				client.delete(new byte[] { i });
			}
			Thread.sleep(1000);
			for (byte i = 0; i < 100; i++) {
				client.get(new byte[] { i });
			}
			Thread.sleep(1000);
			client.incr(new byte[] { 100 }, 0, 10);
			while (true) {
				Thread.sleep(10000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private class Handler implements IKVClientHandler {

		@Override
		public void onDelete(DeleteResult result) {
			try {
				assertEquals(true, result.isSuccess());
			} catch (KVException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void onGet(GetResult result) {
			try {
				byte i = result.getKey()[0];
				if (i % 2 != 0) {
					assertArrayEquals(result.getKey(), result.getValue());
				}
			} catch (KVException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void onIncr(IncrResult result) {
			try {
				assertEquals(10, result.getValue());
			} catch (KVException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void onSet(SetResult result) {
			try {
				assertArrayEquals(result.getKey(), result.getValue());
			} catch (KVException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void onStat(StatResult result) {

		}

	}

}
