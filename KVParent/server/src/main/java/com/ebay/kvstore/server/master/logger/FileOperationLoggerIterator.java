package com.ebay.kvstore.server.master.logger;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import com.ebay.kvstore.logger.FileLoggerInputStream;
import com.ebay.kvstore.logger.ILoggerInputStream;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;

public class FileOperationLoggerIterator implements Iterator<IOperation> {

	private static Logger logger = org.slf4j.LoggerFactory
			.getLogger(FileOperationLoggerIterator.class);

	private ILoggerInputStream in;

	protected IOperation operation;

	public FileOperationLoggerIterator(String path) throws IOException {
		this.in = new FileLoggerInputStream(DFSManager.getDFS().open(new Path(path)));
	}

	@Override
	public boolean hasNext() {
		try {
			byte type = in.read();
			switch (type) {
			case IOperation.Load:
				operation = new LoadOperation();
				break;
			case IOperation.Unload:
				operation = new UnloadOperation();
				break;
			case IOperation.Split:
				operation = new SplitOperation();
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
	public IOperation next() {
		try {
			operation.readFromExternal(in);
			return operation;
		} catch (IOException e) {
			logger.error("Error occured when reading the OperationLoggerFile.", e);
			return null;
		}

	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
