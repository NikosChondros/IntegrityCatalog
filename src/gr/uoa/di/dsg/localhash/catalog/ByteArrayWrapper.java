package gr.uoa.di.dsg.localhash.catalog;

import java.util.Arrays;

import javax.xml.bind.DatatypeConverter;

/**
 * A Class that wraps a byte array and provides value-equality (instead of the
 * default reference-equality) Adapted from:
 * http://stackoverflow.com/questions/1058149
 * /using-a-byte-array-as-hashmap-key-java
 * 
 */
public final class ByteArrayWrapper implements Comparable<ByteArrayWrapper> {
	private final byte[] data;

	public ByteArrayWrapper(byte[] data) {
		if (data == null) {
			throw new NullPointerException();
		}
		this.data = data;
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof ByteArrayWrapper)) {
			return false;
		}
		return Arrays.equals(data, ((ByteArrayWrapper) other).data);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(data);
	}

	public byte[] get() {
		return data;
	}

	@Override
	public int compareTo(ByteArrayWrapper arg0) {
		String current = DatatypeConverter.printHexBinary(data);
		String other = DatatypeConverter.printHexBinary(arg0.get());
		return current.compareTo(other);
	}
	
	@Override
	public String toString() {
		return DatatypeConverter.printHexBinary(data);
	}
}
