package edu.stanford.identiscape.mappedMemory;

public interface IMappedMemory {

	/**
	 * The block size used by the memory manager, measured in bytes.
	 */
	public static final int BLOCKSIZE = 0x10000; // 64 KBytes

	/**
	 * Is this a fresh memory manager? If no block is dirty, and the underlying
	 * file has length 0, then this is a fresh memory manager.
	 */
	public abstract boolean isFresh();

	/**
	 * Soil a block. If the block is in the cache, make it dirty. If the block
	 * is not in the cache, throw an exception.
	 * 
	 * @param diskBlockNumber
	 */
	public abstract void soilBlock(long diskBlockIndex);

	/**
	 * Write a block. If the block is in the cache, make it clean if it wasn't,
	 * and then write it out to disk. Also update the block's access time.
	 */
	public abstract void writeBlock(long diskBlockIndex);

	/** Return the next available new block */
	public abstract long getNewBlockNumber();

	/** Return the current size of the manager */
	public abstract long size();

	/**
	 * Return a block pointing to the given file block.
	 * 
	 * @param diskBlockIndex
	 *            The block number of the requested block on disk
	 * @return A byte array of size BLOCKSIZE holding the contents of the
	 *         requested disk block, or all zeroes if this disk block is new.
	 */
	public abstract byte[] getBlock(long diskBlockIndex);

	/** Flush all dirty buffers. */
	public abstract void flush();

	/** Flush and remove all buffers. */
	public abstract void flushReset();

	/** Close file and release all memory buffers */
	public abstract void close();
}