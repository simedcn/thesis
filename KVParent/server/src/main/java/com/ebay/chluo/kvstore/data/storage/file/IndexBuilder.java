package com.ebay.chluo.kvstore.data.storage.file;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.ebay.chluo.kvstore.KeyValueUtil;
import com.ebay.chluo.kvstore.structure.KeyValue;

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
	public static List<IndexEntry> build(File file, int blockSize, int blockCount)
			throws IOException {
		List<IndexEntry> list = new ArrayList<>();
		KVInputStream in = new KVFileInputStream(new FileInputStream(file), blockSize, 0, 0);
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
				curKey = kv.getKey();
				if (prevKey == null) {
					prevKey = curKey;
				}
				curBlock = in.getCurrentBlock();
				int count = curBlock - prevBlock;
				if (count >= blockCount || (count == blockCount - 1 && in.getBlockAvailable() < 4)) {
					list.add(new IndexEntry(prevKey, curKey, prevBlock, curBlock, offset));
					offset = in.getBlockPos() % blockSize;
					prevBlock = (count == blockCount - 1 ? curBlock + 1 : curBlock);
					prevKey = null;
				}
			}
		} catch (EOFException e) {
			if(prevKey!=null&&curKey!=null){
				list.add(new IndexEntry(prevKey, curKey, prevBlock, curBlock, offset));
			}
			in.close();
		}
		return list;
	}

}
