package com.ebay.kvstore.server.data.storage.fs;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.server.util.KeyValueIOUtil;
import com.ebay.kvstore.structure.KeyValue;

/**
 * Iterate throw a KVFileInputStream in a given range and return a set of
 * KeyValues
 * 
 * @author luochen
 * 
 */
public class KVFileIterator implements Iterator<KeyValue> {

	protected final int blockSize;

	protected final int blockStart;

	protected final int blockEnd;

	protected IBlockInputStream in;

	protected int nextLen;

	private static Logger logger = LoggerFactory.getLogger(KVFileIterator.class);

	public KVFileIterator(int start, int end, int blockSize, int offset, InputStream in)
			throws IOException {
		super();
		this.blockStart = start;
		this.blockEnd = end;
		this.blockSize = blockSize;
		this.in = new KVInputStream(in, blockSize, blockStart, offset);
	}

	public void close() throws IOException {
		in.close();
	}

	@Override
	public boolean hasNext() {
		boolean next = false;
		try {
			if ((in.getCurrentBlock() == blockEnd) && in.getBlockAvailable() < 4) {
				next = false;
			} else {
				nextLen = in.readInt();
				if (nextLen > 0
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
			KeyValue kv = KeyValueIOUtil.readFromExternal(in);
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

}
