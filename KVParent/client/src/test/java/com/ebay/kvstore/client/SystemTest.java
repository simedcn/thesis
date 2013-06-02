package com.ebay.kvstore.client;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.client.result.GetResult;
import com.ebay.kvstore.exception.KVException;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;

public class SystemTest extends BaseClientTest {

	private Random random = new Random();
	private int repeat;
	private Set<byte[]> keys;

	@Before
	public void setUp() throws Exception {
		initClient(new ClientOption(true, 2000, 30, new Address("127.0.0.1", 20000)));
		repeat = 50;
		keys = new HashSet<>();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		while (true) {
			try {
				if (keys.size() > 10000) {
					keys.clear();
				}
				int op = random.nextInt(10);
				long from = System.currentTimeMillis();
				switch (op) {
				case 0:
				case 1:
				case 2:
				case 3:
				case 4:
				case 5:
					set();
					break;
				case 6:
					get();
					break;
				case 7:
					incr();
					break;
				case 8:
					delete();
					break;
				case 9:
					break;
				}
				stat();
				long to = System.currentTimeMillis();
				System.out.println("Time:" + (to - from) + "ms");
				Thread.sleep(1000);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	private void set() throws KVException {
		for (int i = 0; i < repeat; i++) {
			byte[] key = getRandBytes();
			client.set(key, key);
		}
		System.out.println("Set key/values");
	}

	private void get() throws KVException {
		Iterator<byte[]> it = keys.iterator();
		int index = 0;
		while (index < repeat) {
			byte[] key = null;
			boolean exist = false;
			if (random.nextBoolean() && it.hasNext()) {
				key = it.next();
				exist = true;
			} else {
				key = getRandBytes();
			}
			GetResult result = client.get(key);
			if (exist) {
				assertArrayEquals(key, result.getValue());
			}
			index++;
		}

		System.out.println("Get key/values");
	}

	private void delete() throws KVException {
		for (int i = 0; i < repeat; i++) {
			byte[] key = getRandBytes();
			client.delete(key);
		}
		System.out.println("Delete key/values");
	}

	private void incr() throws KVException {
		for (int i = 0; i < repeat; i++) {
			byte[] key = getRandBytes();
			client.incr(key, 1, 0);
		}
		System.out.println("Incr key/values");
	}

	private void stat() throws KVException {
		DataServerStruct[] dataServers = client.stat();
		KVClientMain.formatStat(dataServers, System.out);
	}

	private byte[] getRandBytes() {
		int length = Math.abs(random.nextInt() % 10000) + 10;
		byte[] bytes = new byte[length];
		random.nextBytes(bytes);
		return bytes;
	}

}
