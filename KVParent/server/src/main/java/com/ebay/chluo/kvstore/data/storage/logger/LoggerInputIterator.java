package com.ebay.chluo.kvstore.data.storage.logger;

import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerInputIterator implements Iterator<IMutation> {

	private static Logger logger = LoggerFactory.getLogger(LoggerInputIterator.class);

	protected LoggerInputStream in;

	protected IMutation mutation;

	public LoggerInputIterator(LoggerInputStream in) {
		this.in = in;
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
			logger.error("Error occured when reading the RegionDataFile.", e);
			return null;
		}

	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
