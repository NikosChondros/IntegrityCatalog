package edu.stanford.identiscape.util;

public class ByteArrayRegion {
	public byte[] buffer;
	public int start;
	public int length;
	
	public ByteArrayRegion(byte[] buffer, int start, int length) {
		this.buffer = buffer;
		this.start = start;
		this.length = length;
	}

	/**
	 * Simplified constructor, wraps the complete array passed
	 * @param buffer
	 */
	public ByteArrayRegion(byte[] buffer) {
		this.buffer = buffer;
		this.start = 0;
		this.length = buffer.length;
	}
	
	public void set(byte[] buffer) {
		this.buffer = buffer;
		this.start = 0;
		this.length = buffer.length;
	}
	
	public void set(byte[] buffer, int start, int length) {
		this.buffer = buffer;
		this.start = start;
		this.length = length;
	}

	public byte[] toArray() {
		byte[] ret = new byte[length];
		System.arraycopy(buffer, start, ret, 0, length);
		return ret; 
	}
}
