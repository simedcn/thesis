package com.ebay.kvstore.client;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.structure.Address;

import static org.junit.Assert.*;

public class TestSyncClient extends BaseClientTest {

	public TestSyncClient() {
		initClient(new ClientOption(true, 2000, new Address("127.0.0.1", 20000)));
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
			client.updateRegionTable();

			for (byte i = 0; i < 100; i++) {
				client.set(new byte[] { i }, new byte[] { i });
			}
			for (byte i = 0; i < 100; i += 2) {
				client.delete(new byte[] { i });
			}
			for (byte i = 0; i < 100; i++) {
				byte[] value = client.get(new byte[] { i });
				if (i % 2 != 0) {
					assertArrayEquals(value, new byte[] { i });
				} else {
					assertNull(value);
				}
			}

			client.incr(new byte[] { 100 }, 0, 10);
			assertEquals(client.getCounter(new byte[] { 100 }), 10);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
