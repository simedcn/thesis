package com.ebay.kvstore.server.util;

import com.ebay.kvstore.util.IKVConstants;


/**
 * Used for build file/dir path for specific elements
 * 
 * @author luochen
 * 
 */
public class PathBuilder implements IKVConstants {

	public static String getDataServerDir() {
		return IKVConstants.DFS_Data_Dir;
	}

	public static String getMasterCheckPointDir() {
		StringBuilder sb = new StringBuilder(32);
		sb.append(DFS_Master_Dir);
		sb.append("checkpoint/");
		return sb.toString();
	}

	public static String getMasterCheckPointPath(long time) {
		StringBuilder sb = new StringBuilder(48);
		sb.append(getMasterCheckPointDir());
		sb.append(time);
		sb.append(CheckPoint_Suffix);
		return sb.toString();
	}

	public static String getMasterLogDir() {
		StringBuilder sb = new StringBuilder(32);
		sb.append(DFS_Master_Dir);
		sb.append("log/");
		return sb.toString();
	}

	public static String getMasterLogPath(long time) {
		StringBuilder sb = new StringBuilder(48);
		sb.append(getMasterLogDir());
		sb.append(time);
		sb.append(Log_Suffix);
		return sb.toString();
	}

	public static String getRegionDir(int regionId) {
		StringBuilder sb = new StringBuilder(48);
		sb.append(DFS_Data_Dir);
		sb.append(regionId);
		sb.append('/');
		return sb.toString();
	}

	public static String getRegionFileName(int regionId, long timestamp) {
		StringBuilder sb = new StringBuilder(48);
		sb.append(regionId);
		sb.append('-');
		sb.append(timestamp);
		sb.append(Region_Data_Suffix);
		return sb.toString();
	}

	public static String getRegionFilePath(int regionId, long timestamp) {
		StringBuilder sb = new StringBuilder(48);
		sb.append(getRegionDir(regionId));
		sb.append(getRegionFileName(regionId, timestamp));
		return sb.toString();
	}

	public static String getRegionLogName(int regionId, long timestamp) {
		StringBuilder sb = new StringBuilder(48);
		sb.append(regionId);
		sb.append('-');
		sb.append(timestamp);
		sb.append(Log_Suffix);
		return sb.toString();
	}

	public static String getRegionLogPath(int regionId, long timestamp) {
		StringBuilder sb = new StringBuilder(48);
		sb.append(getRegionDir(regionId));
		sb.append(getRegionLogName(regionId, timestamp));
		return sb.toString();
	}
}
