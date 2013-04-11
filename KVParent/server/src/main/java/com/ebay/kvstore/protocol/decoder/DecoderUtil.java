package com.ebay.kvstore.protocol.decoder;

import org.apache.mina.core.buffer.IoBuffer;

import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.RegionStat;
import com.ebay.kvstore.structure.RegionTable;
import com.ebay.kvstore.structure.SystemInfo;

public class DecoderUtil {
	public static Region decodeRegion(IoBuffer buffer) {
		int regionId = buffer.getInt();
		int length = buffer.getInt();
		byte[] start = new byte[length];
		buffer.get(start);
		length = buffer.getInt();
		byte[] end = null;
		if (length > 0) {
			end = new byte[length];
			buffer.get(end);
		}
		RegionStat stat = decodeRegionStat(buffer);
		Region region = new Region(regionId, start, end);
		region.setStat(stat);
		return region;
	}

	public static RegionStat decodeRegionStat(IoBuffer buffer) {
		RegionStat stat = new RegionStat();
		stat.keyNum = buffer.getInt();
		stat.size = buffer.getLong();
		stat.readCount = buffer.getLong();
		stat.writeCount = buffer.getLong();
		return stat;
	}

	public static Address decodeAddress(IoBuffer buffer) {
		int length = buffer.getInt();
		byte[] bytes = new byte[length];
		buffer.get(bytes);
		String ip = new String(bytes);
		int port = buffer.getInt();
		return new Address(ip, port);
	}

	public static RegionTable decodeRegionTable(IoBuffer buffer) {
		RegionTable table = new RegionTable();
		int size = buffer.getInt();
		for (int i = 0; i < size; i++) {
			Region region = decodeRegion(buffer);
			Address addr = decodeAddress(buffer);
			table.addRegion(region, addr);
		}
		return table;
	}

	public static DataServerStruct decodeDataServer(IoBuffer buffer) {
		Address addr = decodeAddress(buffer);
		int weight = buffer.getInt();
		SystemInfo info = decodeSystemInfo(buffer);
		DataServerStruct dataServer = new DataServerStruct(addr, weight);
		dataServer.setInfo(info);
		int size = buffer.getInt();
		for (int i = 0; i < size; i++) {
			Region region = decodeRegion(buffer);
			dataServer.addRegion(region);
		}
		return dataServer;
	}

	public static SystemInfo decodeSystemInfo(IoBuffer buffer) {
		long memoryFree = buffer.getLong();
		long memoryTotal = buffer.getLong();
		double cpuUsage = buffer.getDouble();
		SystemInfo info = new SystemInfo(memoryTotal, memoryFree, cpuUsage);
		return info;
	}

}
