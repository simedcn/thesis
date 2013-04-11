package com.ebay.kvstore.protocol.encoder;

import org.apache.mina.core.buffer.IoBuffer;

import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.RegionStat;
import com.ebay.kvstore.structure.RegionTable;
import com.ebay.kvstore.structure.SystemInfo;

/**
 * Provide encoding for pojos in com.ebay.kvstore.structure
 * 
 * @author luochen
 * 
 */
public class EncoderUtil {

	/**
	 * Encode {@link Region} Order: int regionId; int start.length; byte[]
	 * start;(int -1;|int end.length;byte[] end;)RegionStat stat;
	 * 
	 * @see EncoderUtil#encodeRegionStat(RegionStat, IoBuffer)
	 * 
	 * @param region
	 * @param buffer
	 */
	public static void encodeRegion(Region region, IoBuffer buffer) {
		buffer.putInt(region.getRegionId());
		buffer.putInt(region.getStart().length);
		buffer.put(region.getStart());
		if (region.getEnd() == null) {
			buffer.putInt(0);
		} else {
			buffer.putInt(region.getEnd().length);
			buffer.put(region.getEnd());
		}
		encodeRegionStat(region.getStat(), buffer);
	}

	/**
	 * Encode {@link RegionStat} Order: int keyNum; long size; long readCount;
	 * long writeCount;
	 * 
	 * @param stat
	 * @param buffer
	 */
	public static void encodeRegionStat(RegionStat stat, IoBuffer buffer) {
		buffer.putInt(stat.keyNum);
		buffer.putLong(stat.size);
		buffer.putLong(stat.readCount);
		buffer.putLong(stat.writeCount);
	}

	/**
	 * Encode {@link Address} Order: int ip.getBytes().length; byte[]
	 * ip.getBytes(); int port;
	 * 
	 * @param stat
	 * @param buffer
	 */
	public static void encodeAddress(Address address, IoBuffer buffer) {
		byte[] bytes = address.ip.getBytes();
		buffer.putInt(bytes.length);
		buffer.put(bytes);
		buffer.putInt(address.port);
	}

	/**
	 * Encode {@link RegionTable} Order: int size; (Region region;Address
	 * addr){size};
	 * 
	 * @param table
	 * @param buffer
	 */
	public static void encodeRegionTable(RegionTable table, IoBuffer buffer) {
		int size = table.getSize();
		buffer.putInt(size);
		for (Region region : table) {
			Address addr = table.getRegionAddr(region);
			encodeRegion(region, buffer);
			encodeAddress(addr, buffer);
		}
	}

	/**
	 * Encode {@link DataServerStruct} Order:Address addr; int weight;
	 * SystemInfo info; int size; (Region region){size};
	 * 
	 * @see EncoderUtil#encodeAddress(Address, IoBuffer)
	 * @see EncoderUtil#encodeRegion(Region, IoBuffer)
	 * @see EncoderUtil#encodeSystemInfo(SystemInfo, IoBuffer)
	 * @param dataServer
	 * @param buffer
	 */
	public static void encodeDataServerStruct(DataServerStruct dataServer, IoBuffer buffer) {
		encodeAddress(dataServer.getAddr(), buffer);
		buffer.putInt(dataServer.getWeight());
		encodeSystemInfo(dataServer.getInfo(), buffer);
		int size = dataServer.getRegions().size();
		buffer.putInt(size);
		for (Region region : dataServer.getRegions()) {
			encodeRegion(region, buffer);
		}
	}

	/**
	 * Encode {@link SystemInfo} Order: long memoryFree; long memoryTotal;
	 * double cpuUsage;
	 * 
	 * @param info
	 * @param buffer
	 */
	public static void encodeSystemInfo(SystemInfo info, IoBuffer buffer) {
		buffer.putLong(info.getMemoryFree());
		buffer.putLong(info.getMemoryTotal());
		buffer.putDouble(info.getCpuUsage());
	}
}
