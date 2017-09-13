package edu.stanford.identiscape.util;

import java.io.DataInput;
import java.io.IOException;

/**
 * ByteSpewer is just an adaptor class for DataInput interface. It implements
 * all interface methods with Unimplemented exceptions.
 */

public class ByteSpewer implements DataInput {

	public ByteSpewer() {
	}

	public void readFully(byte[] parm1) throws IOException {
		/** @todo: Implement this DataInput method */
		throw new UnsupportedOperationException(
				"Method readFully() not yet implemented.");
	}

	public void readFully(byte[] parm1, int parm2, int parm3)
			throws IOException {
		/** @todo: Implement this DataInput method */
		throw new UnsupportedOperationException(
				"Method readFully() not yet implemented.");
	}

	public int skipBytes(int parm1) throws IOException {
		/** @todo: Implement this DataInput method */
		throw new UnsupportedOperationException(
				"Method skipBytes() not yet implemented.");
	}

	public boolean readBoolean() throws IOException {
		/** @todo: Implement this DataInput method */
		throw new UnsupportedOperationException(
				"Method readBoolean() not yet implemented.");
	}

	public byte readByte() throws IOException {
		/** @todo: Implement this DataInput method */
		throw new UnsupportedOperationException(
				"Method readByte() not yet implemented.");
	}

	public int readUnsignedByte() throws IOException {
		/** @todo: Implement this DataInput method */
		throw new UnsupportedOperationException(
				"Method readUnsignedByte() not yet implemented.");
	}

	public short readShort() throws IOException {
		/** @todo: Implement this DataInput method */
		throw new UnsupportedOperationException(
				"Method readShort() not yet implemented.");
	}

	public int readUnsignedShort() throws IOException {
		/** @todo: Implement this DataInput method */
		throw new UnsupportedOperationException(
				"Method readUnsignedShort() not yet implemented.");
	}

	public char readChar() throws IOException {
		/** @todo: Implement this DataInput method */
		throw new UnsupportedOperationException(
				"Method readChar() not yet implemented.");
	}

	public int readInt() throws IOException {
		/** @todo: Implement this DataInput method */
		throw new UnsupportedOperationException(
				"Method readInt() not yet implemented.");
	}

	public long readLong() throws IOException {
		/** @todo: Implement this DataInput method */
		throw new UnsupportedOperationException(
				"Method readLong() not yet implemented.");
	}

	public float readFloat() throws IOException {
		/** @todo: Implement this DataInput method */
		throw new UnsupportedOperationException(
				"Method readFloat() not yet implemented.");
	}

	public double readDouble() throws IOException {
		/** @todo: Implement this DataInput method */
		throw new UnsupportedOperationException(
				"Method readDouble() not yet implemented.");
	}

	public String readLine() throws IOException {
		/** @todo: Implement this DataInput method */
		throw new UnsupportedOperationException(
				"Method readLine() not yet implemented.");
	}

	public String readUTF() throws IOException {
		/** @todo: Implement this DataInput method */
		throw new UnsupportedOperationException(
				"Method readUTF() not yet implemented.");
	}
}
