package com.ebay.kvstore.client;

import java.io.IOException;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import jline.Completor;
import jline.ConsoleReader;

import com.ebay.kvstore.exception.KVException;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

/**
 * Main function for client library
 * 
 * @author luochen
 * 
 */
public class KVClientMain {

	private IKVClient client;

	private static Map<String, String> commands;

	private Address[] masters;

	private int timeout = 2000;

	private String[] args;
	
	public static final int Day = 24 * 3600 * 1000;

	public static final int Hour = 3600 * 1000;

	public static final int Minute = 60 * 1000;

	public static final int Second = 1000;

	public static final int KB = 1024;

	public static final int MB = 1024 * 1024;

	public static final long GB = 1024 * 1024 * 1024;

	static {
		commands = new HashMap<String, String>();
		commands.put("close", "\t(exit the kvclient)");
		commands.put("delete", "key\t(delete the entry that the key specifies)");
		commands.put(
				"incr",
				"key incremental [ttl [initValue]] \t(increment the counter that the key specifies. The default initValue is 0)");
		commands.put("set", "key value [ttl=0]\t(set the value of the key)");
		commands.put("get", "key\t(get the value of the key)");
		commands.put("stat",
				"\t(view statistics information of data servers and regions in the cluster)");
		commands.put("help", "");
	}

	public static void formatStat(DataServerStruct[] dataServers, PrintStream out) {
		if (dataServers == null) {
			out.println("No data server found");
			return;
		}
		for (DataServerStruct server : dataServers) {
			out.println("Data Server: " + server.getAddr());
			indent(out, 1);
			out.println("Info:");
			indent(out, 2);
			out.println("Memory Free:" + server.getInfo().getMemoryFree() /MB + "MB");
			indent(out, 2);
			out.println("Memory Total:" + server.getInfo().getMemoryTotal() / MB
					+ "MB");
			indent(out, 2);
			out.println("Cpu Usage:" + server.getInfo().getCpuUsage() * 100 + "%");
			Collection<Region> regions = server.getRegions();
			indent(out, 1);
			out.println("Regions:");
			for (Region region : regions) {
				indent(out, 2);
				out.print("Region:");
				out.print(region.getRegionId());
				indent(out, 1);
				out.print(Arrays.toString(region.getStart()));
				out.print('-');
				out.print(Arrays.toString(region.getEnd()));
				out.println();
				indent(out, 3);
				out.println("Read Count:" + region.getStat().readCount);
				indent(out, 3);
				out.println("Write Count:" + region.getStat().writeCount);
				indent(out, 3);
				out.println("Entry Num:" + region.getStat().keyNum);
				indent(out, 3);
				out.println("Size:" + formatRegionSize(region.getStat().size));
			}
		}
	}

	public static void main(String[] args) {
		KVClientMain main = new KVClientMain(args);
		main.run();
	}

	private static String formatRegionSize(long size) {
		if (size > MB) {
			return new BigDecimal((double) size /MB).setScale(2, RoundingMode.DOWN)
					+ "MB";
		} else if (size > KB) {
			return new BigDecimal((double) size / KB).setScale(2, RoundingMode.DOWN)
					+ "KB";
		} else {
			return size + "B";
		}
	}

	private static void indent(PrintStream out, int count) {
		for (int i = 0; i < count; i++) {
			out.print('\t');
		}
	}

	public KVClientMain(String[] args) {
		this.args = args;
	}

	public void run() {
		if (!parseArgs(args)) {
			System.err.println("Please use kvstore -h for help");
			return;
		}
		if (masters == null) {
			System.err.println("Error: no master server specified.");
			System.err.println("Please use kvstore -h for help");
			return;
		}
		ClientOption option = new ClientOption(true, timeout, 60, masters);
		client = KVClientFactory.getClient(option);
		process();
		client.close();
	}

	private void doDelete(String[] args) {
		if (args.length != 2) {
			System.err.println("Error usage for delete, please input 'help' for usage");
			return;
		}
		String key = args[1];
		try {
			client.delete(key.getBytes());
			System.err.println("Delete " + key + " success");
		} catch (KVException e) {
			System.err.println("Error:" + e.getMessage());
		}
	}

	private void doGet(String[] args) {
		if (args.length != 2) {
			System.err.println("Error usage for get, please input 'help' for usage");
			return;
		}
		String key = args[1];
		try {
			byte[] result = client.get(key.getBytes()).getValue();
			if (result == null) {
				System.err.println("No value found for " + key);
			} else {
				System.err.println(key + "=" + new String(result));
			}
		} catch (KVException e) {
			System.err.println("Error:" + e.getMessage());
		}
	}

	private void doIncr(String[] args) {
		if (!(args.length >= 2 & args.length <= 5)) {
			System.err.println("Error usage for incr, please input 'help' for usage");
			return;
		}
		String key = args[1];
		int incremental = 1;
		if (args.length >= 3) {
			incremental = Integer.valueOf(args[2]);
		}
		int ttl = 0;
		if (args.length >= 4) {
			ttl = Integer.valueOf(args[3]);
		}
		int initValue = 0;
		if (args.length == 5) {
			initValue = Integer.valueOf(args[4]);
		}
		try {
			int result = client.incr(key.getBytes(), incremental, initValue, ttl);
			System.err.println(key + "=" + result);
		} catch (KVException e) {
			System.err.println("Error:" + e.getMessage());
		}
	}

	private void doSet(String[] args) {
		if (args.length != 3 && args.length != 4) {
			System.err.println("Error usage for set, please input 'help' for usage");
			return;
		}
		String key = args[1];
		String value = args[2];
		int ttl = 0;
		if (args.length == 4) {
			ttl = Integer.valueOf(args[3]);
		}
		try {
			client.set(key.getBytes(), value.getBytes(), ttl);
			System.err.println("Set " + key + "=" + value + " success");
		} catch (KVException e) {
			System.err.println("Error:" + e.getMessage());
		}
	}

	private void doStat(String[] args) {
		if (args.length != 1) {
			System.err.println("Error usage for stat, please input 'help' for usage");
			return;
		}
		try {
			DataServerStruct[] dataServers = client.stat();
			formatStat(dataServers, System.err);
		} catch (KVException e) {
			System.err.println("Error:" + e.getMessage());
		}
	}

	private boolean parseArgs(String[] args) {
		List<String> argList = Arrays.asList(args);
		Iterator<String> it = argList.iterator();
		while (it.hasNext()) {
			String opt = it.next();
			try {
				if (opt.equals("-server")) {
					String addr = it.next();
					String[] addrs = addr.split(",");
					masters = new Address[addrs.length];
					for (int i = 0; i < addrs.length; i++) {
						masters[i] = Address.parse(addrs[i]);
					}
				} else if (opt.equals("-timeout")) {
					timeout = Integer.parseInt(it.next());
				} else if (opt.equals("-h")) {
					usage();
					System.exit(0);
				} else {
					System.err.println("Error: unknown argument " + opt);
					return false;
				}
			} catch (NoSuchElementException e) {
				System.err.println("Error: no argument found for option " + opt);
				return false;
			} catch (Exception e) {
				System.err.println("Error: invalid format for argument " + opt);
				return false;
			}
		}
		return true;
	}

	private void parseLine(String line) {
		String[] args = line.split(" ");
		if (args.length == 0) {
			return;
		}
		String command = args[0];
		if (command.equals("set")) {
			doSet(args);
		} else if (command.equals("get")) {
			doGet(args);
		} else if (command.equals("incr")) {
			doIncr(args);
		} else if (command.equals("delete")) {
			doDelete(args);
		} else if (command.equals("stat")) {
			doStat(args);
		} else if (command.equals("help")) {
			usage();
		} else if (command.equals("close")) {
			System.err.println("Bye bye");
			client.close();
			System.exit(0);
		} else {
			System.err.println("Error:Unknown command:" + command);
		}
	}

	private void process() {
		System.err.println("Welcome to KVStore");
		try {
			ConsoleReader reader = new ConsoleReader();
			reader.setBellEnabled(false);
			reader.addCompletor(new CommandCompletor());
			String line = null;
			while ((line = reader.readLine("kvstore>")) != null) {
				if (!line.isEmpty()) {
					parseLine(line);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void usage() {
		System.err
				.println("kvstore -server host:port[,host:port[,host:port]...] [-timeout time] [-h]");
		for (String cmd : commands.keySet()) {
			System.err.println("\t" + cmd + " " + commands.get(cmd));
		}
	}

	private class CommandCompletor implements Completor {
		@Override
		public int complete(String buffer, int cursor, List candidates) {
			buffer = buffer.substring(0, cursor);
			String token = "";
			if (!buffer.endsWith(" ")) {
				String[] tokens = buffer.split(" ");
				if (tokens.length != 0) {
					token = tokens[tokens.length - 1];
				}
			}
			return completeCommand(buffer, token, candidates);
		}

		private int completeCommand(String buffer, String token, List<String> candidates) {
			for (String cmd : commands.keySet()) {
				if (cmd.startsWith(token)) {
					candidates.add(cmd);
				}
			}
			return buffer.lastIndexOf(" ") + 1;
		}
	}
}
