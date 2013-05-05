package com.ebay.kvstore.server.data.logger;

import java.io.EOFException;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.server.logger.FileLoggerInputStream;
import com.ebay.kvstore.server.logger.ILoggerInputStream;
import com.ebay.kvstore.server.util.DFSManager;

public class DataFileLoggerIterator implements Iterator<IMutation> {

	private static Logger logger = LoggerFactory.getLogger(DataFileLoggerIterator.class);

	protected ILoggerInputStream in;

	protected IMutation mutation;

	protected String path;

	public DataFileLoggerIterator(String path) throws IOException {
		this.in = new FileLoggerInputStream(DFSManager.getDFS().open(new Path(path)));
		this.path = path;
	}

	@Override
	public boolean hasNext() {
		try {
			byte type = in.read();
			switch (type) {
			case IMutation.Set:
				mutation = new SetMutation(null, null);
				break;
			case IMutation.Delete:
				mutation = new DeleteMutation(null);
				break;
			default:
				in.close();
				return false;
			}
		} catch (EOFException eof) {
			try {
				in.close();
			} catch (IOException e1) {
			}
			return false;
		} catch (IOException e) {
			logger.error("Error occured when parsing the LoggerFile.", e);
			try {
				in.close();
			} catch (IOException e1) {
			}
			return false;
		}
		return true;
	}

	@Override
	public IMutation next() {
		try {
			mutation.readFromExternal(in);
			return mutation;
		} catch (IOException e) {
			logger.error("Error occured when reading the RegionLoggerFile.", e);
		}
		return null;

	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
