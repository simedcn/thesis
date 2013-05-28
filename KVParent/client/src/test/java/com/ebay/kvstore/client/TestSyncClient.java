package com.ebay.kvstore.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.client.result.GetResult;
import com.ebay.kvstore.structure.Address;

public class TestSyncClient extends BaseClientTest {

	public TestSyncClient() {
		initClient(new ClientOption(true, 2000, 30, new Address("127.0.0.1", 20000)));
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
			for (byte i = 0; i < 100; i += 2) {
				client.delete(new byte[] { i });
			}
			for (byte i = 0; i < 100; i++) {
				GetResult result = client.get(new byte[] { i });
				if (i % 2 != 0) {
					assertArrayEquals(result.getValue(), new byte[] { i });
					assertEquals(0, result.getTtl());
				} else {
					assertNull(result.getValue());
				}
			}
			client.delete(new byte[]{100});
			client.incr(new byte[] { 100 }, 10, 0);
			assertEquals(client.getCounter(new byte[] { 100 }).getCounter(), 10);

			byte[] key1 = new byte[] { 0 };
			client.set(key1, key1, 1000);
			Thread.sleep(500);
			GetResult result = client.get(key1);
			assertArrayEquals(key1, result.getValue());
			assertEquals(true, result.getTtl() > 0);
			Thread.sleep(500);
			result = client.get(key1);
			assertNull(result.getValue());

			byte[] key2 = new byte[] { 100 };
			client.incr(key2, 10, 0, 1000);
			result = client.get(key2);
			assertEquals(20, result.getCounter());
			Thread.sleep(1000);
			int counter = client.incr(key2, 10, 0);
			assertEquals(10, counter);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
