package edu.stanford.identiscape.util;

/**
 * A thread local buffer is a byte array initialized and used within a class as
 * a static. However, a different buffer is used per calling thread.
 */
public class ThreadLocalBuffer extends ThreadLocal {
	/** The size of the buffer in bytes */
	private int size_;

	/** Create a thread local buffer, given its size */
	public ThreadLocalBuffer(int size) {
		// It'd better be greater than 0
		if (size <= 0) {
			throw new RuntimeException("Invalid buffers size " + size);
		}
		size_ = size;
	}

	/** From ThreadLocal. Create a new buffer of the given size */
	protected Object initialValue() {
		return new byte[size_];
	}
}
