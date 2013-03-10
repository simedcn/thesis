package com.ebay.chluo.kvstore.data.storage.file;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.chluo.kvstore.KeyValueUtil;
import com.ebay.chluo.kvstore.structure.KeyValue;

/**
 * Iterate throw a KVFileInputStream in a given range and return a set of
 * KeyValues
 * 
 * @author luochen
 * 
 */
public class KeyValueInputIterator implements Iterator<KeyValue> {

	protected final int blockSize;

	protected final int blockStart;

	protected final int blockEnd;

	protected KVInputStream in;

	protected int nextLen;

	private static Logger logger = LoggerFactory.getLogger(KeyValueInputIterator.class);

	public KeyValueInputIterator(int start, int end, int blockSize, int offset, File file)
			throws IOException {
		super();
		this.blockStart = start;
		this.blockEnd = end;
		this.blockSize = blockSize;
		this.in = new KVFileInputStream(new FileInputStream(file), blockSize, blockStart, offset);
	}

	@Override
	public boolean hasNext() {
		boolean next = false;
		try {
			if ((in.getCurrentBlock() == blockEnd) && in.getBlockAvailable() < 4) {
				next = false;
			} else {
				nextLen = in.readInt();
				if (nextLen != 0
						&& ((blockEnd < 0) || (nextLen + in.getPos() <= (blockEnd + 1) * blockSize))) {
					next = true;
					in.skipBytes(-4);
				}
			}
		} catch (EOFException e) {
			next = false;
		} catch (IOException e) {
			logger.error("Error occured when parsing the RegionDataFile.", e);
			next = false;
		}
		if (!next) {
			try {
				in.close();
			} catch (Exception e) {
				logger.error("Error occurred when closing the RegionDataFile", e);
			}
		}
		return next;
	}

	@Override
	public KeyValue next() {
		try {
			KeyValue kv = KeyValueUtil.readFromExternal(in);
			return kv;
		} catch (IOException e) {
			logger.error("Error occured when reading the RegionDataFile.", e);
			return null;
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	public void close() throws IOException {
		in.close();
	}

}
