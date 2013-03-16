package com.ebay.kvstore.server.data.storage.fs;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * 
 * @author luochen
 * 
 */
public class KVInputStream extends FilterInputStream implements IBlockInputStream {

	protected final int blockSize;

	protected int currentBlock;

	protected ByteArrayInputStream blockStream; // load a block into memory to
												// perform reading

	protected boolean eof = false;

	protected byte[] blockBuffer;

	public KVInputStream(InputStream in, int blockSize, int startBlock, int offset)
			throws IOException {
		super(in);
		if (offset >= blockSize) {
			throw new IllegalArgumentException("Offset " + offset + " should less than blockSize "
					+ blockSize);
		}
		this.blockSize = blockSize;
		this.currentBlock = startBlock;
		in.skip(blockSize * startBlock);
		blockBuffer = new byte[blockSize];
		in.read(blockBuffer);
		blockStream = new ByteArrayInputStream(blockBuffer);
		blockStream.skip(offset);
	}

	/**
	 * This method reads a block from file and fills the blockStream, the old
	 * blockStream will be deprecated. Note that if a new block is loaded, the
	 * old ones are overrode by it.
	 * 
	 * @throws IOException
	 */
	private void readNextBlock() throws IOException {
		if (eof) {
			throw new EOFException();
		}

		currentBlock++;
		if (in.read(blockBuffer, 0, blockSize) < blockSize) {
			// the file has reached the end
			eof = true;
		}
		blockStream = new ByteArrayInputStream(blockBuffer);
	}

	// pos = blockSize*currentBlock + blockStream.pos()
	public int getPos() {
		return blockSize * (currentBlock + 1) - blockStream.available();
	}

	public int getBlockPos() {
		return blockSize - blockStream.available();
	}

	public int getCurrentBlock() {
		return currentBlock;
	}

	public int getBlockAvailable() {
		return blockStream.available();
	}

	@Override
	public void close() throws IOException {
		super.close();
		blockStream.close();
	}

	@Override
	public void readFully(byte[] b) throws IOException {
		if (blockStream.available() >= b.length) {
			blockStream.read(b);
		} else {
			int toRead = blockStream.available();
			int read = 0;
			for (;;) {
				blockStream.read(b, read, toRead);
				read += toRead;
				if (read >= b.length) {
					break;
				}
				readNextBlock();
				toRead = blockStream.available() - (b.length - read) >= 0 ? blockStream.available()
						: (b.length - read);
			}
		}
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
			throw new IndexOutOfBoundsException();
		} else if (len == 0) {
			return;
		}
		if (blockStream.available() >= len) {
			blockStream.read(b, off, len);
		} else {
			int toRead = blockStream.available();
			int read = 0;
			for (;;) {
				blockStream.read(b, off + read, toRead);
				read += toRead;
				if (read >= len) {
					break;
				}
				readNextBlock();
				toRead = (blockStream.available() - (len - read)) >= 0 ? (len - read) : blockStream
						.available();
			}
		}
	}

	/**
	 * Skip n bytes, n could either be positive or negative. if n < 0, make sure
	 * n<= {@link KVInputStream#getBlockPos()}. if n>0, n should not exceed the
	 * file size limit.
	 */
	@Override
	public int skipBytes(int n) throws IOException {
		if (n > 0) {
			// skip forword
			if (blockStream.available() >= n) {
				blockStream.skip(n);
			} else {
				int skip = 0;
				int toSkip = blockStream.available();
				for (;;) {
					if (blockStream.available() >= toSkip) {
						blockStream.skip(toSkip);
						return n;
					} else {
						skip += blockStream.available();
						readNextBlock();
						toSkip = n - skip;
					}
				}
			}
		} else {
			int curPos = getBlockPos();
			int index = curPos + n;
			if (index < 0) {
				throw new IndexOutOfBoundsException("Cannot skip to an old block");
			}
			blockStream.reset();
			blockStream.skip(index);

		}
		return 0;
	}

	@Override
	public int readInt() throws IOException {
		if (blockStream.available() < 4) {
			readNextBlock();
		}
		int ch1 = blockStream.read();
		int ch2 = blockStream.read();
		int ch3 = blockStream.read();
		int ch4 = blockStream.read();
		if ((ch1 | ch2 | ch3 | ch4) < 0)
			throw new EOFException();
		return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
	}

	@Override
	public byte readByte() throws IOException {
		if (blockStream.available() < 1) {
			readNextBlock();
		}
		int ch = blockStream.read();
		return (byte) (ch);
	}

	@Override
	public int readUnsignedByte() throws IOException {
		if (blockStream.available() < 1) {
			readNextBlock();
		}
		int ch = blockStream.read();
		return (byte) (ch);
	}

	@Override
	public boolean readBoolean() throws IOException {
		return false;
	}

	@Override
	public short readShort() throws IOException {
		return 0;
	}

	@Override
	public int readUnsignedShort() throws IOException {
		return 0;
	}

	@Override
	public char readChar() throws IOException {
		return 0;
	}

	@Override
	public long readLong() throws IOException {
		return 0;
	}

	@Override
	public float readFloat() throws IOException {
		return 0;
	}

	@Override
	public double readDouble() throws IOException {
		return 0;
	}

	@Override
	public String readLine() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String readUTF() throws IOException {
		throw new UnsupportedOperationException();

	}

}
