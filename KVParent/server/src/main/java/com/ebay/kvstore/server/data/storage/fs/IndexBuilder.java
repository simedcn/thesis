package com.ebay.kvstore.server.data.storage.fs;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;

import com.ebay.kvstore.KeyValueUtil;
import com.ebay.kvstore.structure.KeyValue;

public class IndexBuilder {

	/**
	 * Build the index on the given file.
	 * 
	 * @param file
	 *            , RegionDataFile, should be sorted on key in ascend order
	 * @param blockSize
	 * @param blockCount
	 *            the approximate blocks in each index entry.This factor is
	 *            useful to balance the key/values in each index entry,and also
	 *            have a influence on how many blocks the system load when the
	 *            system try to load data from file system.
	 * @return
	 * @throws IOException
	 */
	public static int build(List<IndexEntry> list, String file, int blockSize, int blockCount)
			throws IOException {
		IBlockInputStream in = new KVInputStream(DFSManager.getDFS().open(new Path(file)),
				blockSize, 0, 0);
		int keyNum = 0;
		byte[] prevKey = null, curKey = null;
		int prevBlock = 0, curBlock = 0;
		int offset = 0;
		KeyValue kv = null;
		try {
			while (true) {
				int len = in.readInt();
				if (len == 0) {
					// prevKey == null means the index entry has been flushed
					// before
					if (prevKey != null) {
						list.add(new IndexEntry(prevKey, curKey, prevBlock, curBlock, offset));
					}
					in.close();
					break;
				} else {
					in.skipBytes(-4);
				}
				kv = KeyValueUtil.readFromExternal(in);
				keyNum++;
				curKey = kv.getKey();
				if (prevKey == null) {
					prevKey = curKey;
				}
				curBlock = in.getCurrentBlock();
				int count = curBlock - prevBlock;
				if (count >= blockCount || (count == blockCount - 1 && in.getBlockAvailable() < 4)) {
					list.add(new IndexEntry(prevKey, curKey, prevBlock, curBlock, offset));
					offset = in.getBlockPos() % blockSize;
					prevBlock = curBlock;
					if (count == blockCount - 1) {
						prevBlock++;
						offset = 0;
					}
					prevKey = null;
				}
			}
		} catch (EOFException e) {
			if (prevKey != null && curKey != null) {
				list.add(new IndexEntry(prevKey, curKey, prevBlock, curBlock, offset));
			}
			in.close();
		}
		return keyNum;
	}

}
