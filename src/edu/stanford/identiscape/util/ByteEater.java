package edu.stanford.identiscape.util;

import java.io.DataOutput;
import java.io.IOException;

/**
 * ByteEater is just an adaptor class for DataOutput implementations. It
 * implements all interface methods with Unimplemented exceptions.
 */
public class ByteEater implements DataOutput {
	public void write(int b) throws IOException {
		/** @todo: Implement this java.io.DataOutput method */
		throw new UnsupportedOperationException(
				"Method write() not yet implemented.");
	}

	public void write(byte[] b) throws IOException {
		/** @todo: Implement this java.io.DataOutput method */
		throw new UnsupportedOperationException(
				"Method write() not yet implemented.");
	}

	public void write(byte[] b, int off, int len) throws IOException {
		/** @todo: Implement this java.io.DataOutput method */
		throw new UnsupportedOperationException(
				"Method write() not yet implemented.");
	}

	public void writeBoolean(boolean v) throws IOException {
		/** @todo: Implement this java.io.DataOutput method */
		throw new UnsupportedOperationException(
				"Method writeBoolean() not yet implemented.");
	}

	public void writeByte(int v) throws IOException {
		/** @todo: Implement this java.io.DataOutput method */
		throw new UnsupportedOperationException(
				"Method writeByte() not yet implemented.");
	}

	public void writeShort(int v) throws IOException {
		/** @todo: Implement this java.io.DataOutput method */
		throw new UnsupportedOperationException(
				"Method writeShort() not yet implemented.");
	}

	public void writeChar(int v) throws IOException {
		/** @todo: Implement this java.io.DataOutput method */
		throw new UnsupportedOperationException(
				"Method writeChar() not yet implemented.");
	}

	public void writeInt(int v) throws IOException {
		/** @todo: Implement this java.io.DataOutput method */
		throw new UnsupportedOperationException(
				"Method writeInt() not yet implemented.");
	}

	public void writeLong(long v) throws IOException {
		/** @todo: Implement this java.io.DataOutput method */
		throw new UnsupportedOperationException(
				"Method writeLong() not yet implemented.");
	}

	public void writeFloat(float v) throws IOException {
		/** @todo: Implement this java.io.DataOutput method */
		throw new UnsupportedOperationException(
				"Method writeFloat() not yet implemented.");
	}

	public void writeDouble(double v) throws IOException {
		/** @todo: Implement this java.io.DataOutput method */
		throw new UnsupportedOperationException(
				"Method writeDouble() not yet implemented.");
	}

	public void writeBytes(String s) throws IOException {
		/** @todo: Implement this java.io.DataOutput method */
		throw new UnsupportedOperationException(
				"Method writeBytes() not yet implemented.");
	}

	public void writeChars(String s) throws IOException {
		/** @todo: Implement this java.io.DataOutput method */
		throw new UnsupportedOperationException(
				"Method writeChars() not yet implemented.");
	}

	public void writeUTF(String str) throws IOException {
		/** @todo: Implement this java.io.DataOutput method */
		throw new UnsupportedOperationException(
				"Method writeUTF() not yet implemented.");
	}
}
