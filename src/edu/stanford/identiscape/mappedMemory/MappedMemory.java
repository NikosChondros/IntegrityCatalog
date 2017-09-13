package edu.stanford.identiscape.mappedMemory;

import java.io.DataOutput;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeSet;

import edu.stanford.identiscape.util.Bytes;
import edu.stanford.identiscape.util.ThreadLocalBuffer;

/**
 * This is a simplistic memory-mapped file buffer manager. It allows a
 * non-native mapping of an entire file onto a limited memory window. The block
 * size for disk accesses is predetermined. Buffer blocks are replaced using the
 * LRU algorithm.
 */
public class MappedMemory implements IMappedMemory {
	/** The file itself */
	private RandomAccessFile file_;

	/** The size of the buffer cache */
	private int cacheSize_;

	/** The actual buffer blocks */
	private byte[][] blocks_;

	/** The dirty bits of buffer blocks */
	private boolean[] dirty_;

	/** The block access entries used to enforce LRU */
	private BlockAccessEntry[] blockAccessEntries_;

	/** The access time priority queue. Entries are BlockAccessEntries */
	private TreeSet priorityQueue_;

	/**
	 * The directory of block access entries indexed by disk block number
	 */
	private HashMap diskBlockDirectory_;

	/**
	 * A utility entry used for searching in the disk block directory
	 */
	private BlockAccessEntry directorySearcher_;

	/** The first unused block number */
	private int firstUnused_;

	/**
	 * The logical clock used to prioritize buffer blocks when replacing.
	 */
	private long time_;

	/** The next new block available */
	private long nextNewBlock_;

	/**
	 * Create a new memory buffer, given a file on disk, and the size of the
	 * buffer cache. If the file does not exist, a new one is created.
	 * 
	 * @param fileName
	 *            The name of the mapped file.
	 * @param cacheSize
	 *            The size of the buffer cache, measured in blocks. This should
	 *            be at least 1.
	 */
	public MappedMemory(String fileName, int cacheSize) {
		try {
			// Open the named file. If it doesn't exist, create a
			// new one
			file_ = new RandomAccessFile(fileName, "rw"); // read-write access
		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new RuntimeException("File problems");
		}

		// Check the cache size
		if (cacheSize < 1) {
			throw new RuntimeException("Cache size must be " + "at least 1");
		}
		cacheSize_ = cacheSize;

		// Initialize the buffer blocks
		blocks_ = new byte[cacheSize_][BLOCKSIZE];

		// Initialize the dirty bits
		dirty_ = new boolean[cacheSize_];

		// Initialize all block access entries
		blockAccessEntries_ = new BlockAccessEntry[cacheSize_];
		for (int i = 0; i < cacheSize_; i++) {
			blockAccessEntries_[i] = new BlockAccessEntry(i, 0);
		}

		// The priority queue has no one in it
		priorityQueue_ = new TreeSet(BlockAccessEntry.ACCESSCOMPARATOR);

		// The directory of disk blocks
		diskBlockDirectory_ = new HashMap();

		// The first unused block is the first block
		firstUnused_ = 0;

		// And the time is 0
		time_ = 0;

		// Now initialize the next new block, based on the startup
		// length of the file.
		nextNewBlock_ = calculateNewBlockNumber();

		// Initialize the directory searcher
		directorySearcher_ = new BlockAccessEntry(0, 0);
	}

	/* (non-Javadoc)
	 * @see edu.stanford.identiscape.mappedMemory.IMappedMemory#isFresh()
	 */
	@Override
	public boolean isFresh() {
		// Check dirty flags
		for (int i = 0; i < cacheSize_; i++) {
			if (dirty_[i]) {
				return false;
			}
		}

		// No dirty flags. Check the next new
		return nextNewBlock_ == 0;
	}

	/* (non-Javadoc)
	 * @see edu.stanford.identiscape.mappedMemory.IMappedMemory#soilBlock(long)
	 */
	@Override
	public void soilBlock(long diskBlockIndex) {
		// Do I have a buffer of the block?
		directorySearcher_.diskBlockNumber = diskBlockIndex;
		BlockAccessEntry entry = (BlockAccessEntry) diskBlockDirectory_
				.get(directorySearcher_);
		if (entry == null) {
			throw new RuntimeException("Can't soil an " + "uncached block");
		} else {
			dirty_[entry.getBlockNumber()] = true;
		}
	}

	/* (non-Javadoc)
	 * @see edu.stanford.identiscape.mappedMemory.IMappedMemory#writeBlock(long)
	 */
	@Override
	public void writeBlock(long diskBlockIndex) {
		// Do I have a buffer of the block?
		directorySearcher_.diskBlockNumber = diskBlockIndex;
		BlockAccessEntry entry = (BlockAccessEntry) diskBlockDirectory_
				.get(directorySearcher_);
		if (entry == null) {
			throw new RuntimeException("Can't write an " + "uncached block");
		} else {
			time_++;
			int blockNumber = entry.getBlockNumber();
			dirty_[blockNumber] = false;

			// Update priority queue
			priorityQueue_.remove(blockAccessEntries_[blockNumber]);
			blockAccessEntries_[blockNumber].accessTime = time_;
			priorityQueue_.add(blockAccessEntries_[blockNumber]);

			// And flush out the contents of the block
			try {
				file_.seek(entry.diskBlockNumber * BLOCKSIZE);
				file_.write(blocks_[blockNumber]);
			} catch (IOException ioe) {
				ioe.printStackTrace();
				throw new RuntimeException("File write " + "croaked");
			}
		}
	}

	/* (non-Javadoc)
	 * @see edu.stanford.identiscape.mappedMemory.IMappedMemory#getNewBlockNumber()
	 */
	@Override
	public long getNewBlockNumber() {
		long newNextNewBlock = nextNewBlock_;
		nextNewBlock_++;
		return newNextNewBlock;
	}

	/* (non-Javadoc)
	 * @see edu.stanford.identiscape.mappedMemory.IMappedMemory#size()
	 */
	@Override
	public long size() {
		try {
			return file_.length();
		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new RuntimeException("File croaked");
		}
	}

	/**
	 * Return the number of the next empty block after the end of the file.
	 */
	private long calculateNewBlockNumber() {
		// Get the size of the file
		long length;
		try {
			length = file_.length();
		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new RuntimeException("File croaked");
		}

		// What's the next block right after the last file byte
		// (length - 1)? If the length of the file is a multiple of
		// the block size, this is the next block start
		long newBlock;
		if (length % BLOCKSIZE == 0) {
			newBlock = length / BLOCKSIZE;
		} else {
			// The next block starts after the end of the block
			// containing the length of the file
			newBlock = (length / BLOCKSIZE) + 1L;
		}

		return newBlock;
	}

	/* (non-Javadoc)
	 * @see edu.stanford.identiscape.mappedMemory.IMappedMemory#getBlock(long)
	 */
	@Override
	public byte[] getBlock(long diskBlockIndex) {
		// Move time forward
		time_++;

		// Do I have a buffer of the block?
		directorySearcher_.diskBlockNumber = diskBlockIndex;
		BlockAccessEntry entry = (BlockAccessEntry) diskBlockDirectory_
				.get(directorySearcher_);
		if (entry == null) {
			// We don't have such an entry
			int blockNumber; // The block number where I'll bring
								// in the requested buffer

			// Do we have any free entries?
			if (firstUnused_ < cacheSize_) {
				// Yup. Put the buffer there
				blockNumber = firstUnused_;
				entry = blockAccessEntries_[blockNumber];

				// And advance the unused pointer
				firstUnused_++;
			} else {
				// No free entries. Pick the first one in the
				// priority queue. There should certainly be one!
				entry = (BlockAccessEntry) priorityQueue_.first();
				priorityQueue_.remove(entry);

				// Also remove it from the directory
				diskBlockDirectory_.remove(entry);

				blockNumber = entry.getBlockNumber();

				// If the entry is dirty write it out
				if (dirty_[blockNumber]) {
					try {
						file_.seek(entry.diskBlockNumber * BLOCKSIZE);
						file_.write(blocks_[blockNumber]);
					} catch (IOException ioe) {
						ioe.printStackTrace();
						throw new RuntimeException("File write " + "croaked");
					}
				}
			}

			// Now bring the missing block in
			entry.diskBlockNumber = diskBlockIndex;
			entry.accessTime = time_;

			// Stick it into the priority queue and the
			// directory
			diskBlockDirectory_.put(entry, entry);
			priorityQueue_.add(entry);

			// Make sure the dirty bit is unset
			dirty_[blockNumber] = false;

			// Bring in the buffer. If it doesn't exist yet, just
			// fill up the buffer with zeroes.
			try {
				if (file_.length() < (diskBlockIndex + 1) * BLOCKSIZE) {
					Arrays.fill(blocks_[blockNumber], (byte) 0);
				} else {
					file_.seek(diskBlockIndex * BLOCKSIZE);
					file_.readFully(blocks_[blockNumber]);
				}
			} catch (IOException ioe) {
				ioe.printStackTrace();
				throw new RuntimeException("File croaked");
			}

			// Update the next new block number to be at least the
			// one after the newly requested one
			if (diskBlockIndex >= nextNewBlock_) {
				nextNewBlock_ = diskBlockIndex + 1;
			}

			// Finally return the buffer
			return blocks_[blockNumber];
		} else {
			// We have that entry. Update its request time
			priorityQueue_.remove(entry);
			entry.accessTime = time_;
			priorityQueue_.add(entry);

			// And return the buffer
			return blocks_[entry.getBlockNumber()];
		}
	}

	/* (non-Javadoc)
	 * @see edu.stanford.identiscape.mappedMemory.IMappedMemory#flush()
	 */
	@Override
	public void flush() {
		try {
			// Flush out dirty blocks
			for (int i = 0; i < cacheSize_; i++) {
				if (dirty_[i]) {
					file_.seek(blockAccessEntries_[i].diskBlockNumber
							* BLOCKSIZE);
					file_.write(blocks_[i]);
					dirty_[i] = false;
				}
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new RuntimeException("File croaked");
		}
	}

	/* (non-Javadoc)
	 * @see edu.stanford.identiscape.mappedMemory.IMappedMemory#flushReset()
	 */
	@Override
	public void flushReset() {
		try {
			// Flush out dirty blocks
			for (int i = 0; i < cacheSize_; i++) {
				if (dirty_[i]) {
					file_.seek(blockAccessEntries_[i].diskBlockNumber
							* BLOCKSIZE);
					file_.write(blocks_[i]);
					dirty_[i] = false;
				}
			}
			diskBlockDirectory_.clear();
			priorityQueue_.clear();
			firstUnused_ = 0;
		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new RuntimeException("File croaked");
		}
	}

	// Convenient static methods for our clients: To/From memory
	// //////////////////////////////////////////////////////////

	/**
	 * Copy a portion of a byte array onto memory. The buffer array is assumed
	 * to be large enough for the values used (i.e., it's length is expected to
	 * be at least offset+size).
	 * 
	 * <P>
	 * 
	 * @param manager
	 *            The manager used.
	 * @param buffer
	 *            The buffer from which data are copied out.
	 * @param offset
	 *            The offset into the buffer where the copying starts.
	 * @param size
	 *            The number of bytes to copy.
	 * @param firstByte
	 *            The first byte in the manager to which data bytes are copies.
	 */
	public static void copyToMemory(IMappedMemory manager, byte[] buffer,
			int offset, int size, long firstByte) {
		// Find the next block to copy into
		long nextBlock = firstByte / BLOCKSIZE;
		int relativeFirstByte = (int) (firstByte % BLOCKSIZE);
		int chunkSize;
		byte[] block;

		while (size > 0) {
			// Get the next block and soil it
			block = manager.getBlock(nextBlock);
			manager.soilBlock(nextBlock);

			if (size + relativeFirstByte > BLOCKSIZE) {
				chunkSize = BLOCKSIZE - relativeFirstByte;
			} else {
				chunkSize = size;
			}

			// And copy the rest there
			System.arraycopy(buffer, offset, block, relativeFirstByte,
					chunkSize);
			size -= chunkSize;
			offset += chunkSize;
			relativeFirstByte = 0;
			nextBlock++;
		}
	}

	/**
	 * A convenience method that reads a number of bytes from the memory manager
	 * and puts them in the given byte array. The source buffer is not checked
	 * for size.
	 */
	public static void copyFromMemory(IMappedMemory manager, long firstByte,
			byte[] buffer, int offset, int size) {
		// Find the block containing the first byte, and the offset of
		// the first byte in that block
		long nextBlock = firstByte / BLOCKSIZE;
		int relativeFirstByte = (int) (firstByte % BLOCKSIZE);
		byte[] block;
		int chunkSize;

		while (size > 0) {
			// Get the first block and soil it
			block = manager.getBlock(nextBlock);

			// Feed as much as can be read from the current block
			if (BLOCKSIZE > relativeFirstByte + size) {
				chunkSize = size;
			} else {
				chunkSize = BLOCKSIZE - relativeFirstByte;
			}
			System.arraycopy(block, relativeFirstByte, buffer, offset,
					chunkSize);

			// Update the remainder
			size -= chunkSize;
			offset += chunkSize;
			nextBlock++;
			relativeFirstByte = 0;
		}
	}

	/**
	 * A convenience method that reads a number of bytes from the memory manager
	 * and writes them into the given data output implementer.
	 */
	public static void writeFromMemory(IMappedMemory manager, long firstByte,
			int size, DataOutput eater) {
		// Find the block containing the first byte, and the
		// offset of the first byte in that block
		long nextBlock = firstByte / BLOCKSIZE;
		int relativeFirstByte = (int) (firstByte % BLOCKSIZE);
		byte[] block;
		int chunkSize;

		while (size > 0) {
			// Get the first block and soil it
			block = manager.getBlock(nextBlock);

			// Feed as much as can be read from the current block
			if (BLOCKSIZE > relativeFirstByte + size) {
				chunkSize = size;
			} else {
				chunkSize = (int) (BLOCKSIZE - relativeFirstByte);
			}
			try {
				eater.write(block, relativeFirstByte, chunkSize);
			} catch (IOException ioe) {
				throw new RuntimeException("Should not happen");
			}

			// Update the remainder
			size -= chunkSize;
			nextBlock++;
			relativeFirstByte = 0;
		}
	}

	/**
	 * Convenience buffer for number to memory copies. There's one for each
	 * calling thread.
	 */
	private static ThreadLocalBuffer numberBytesThreadLocal_ = new ThreadLocalBuffer(
			8);

	/**
	 * Copy a long onto memory. Just patches into the copyToMemory method.
	 */
	public static void longToMemory(IMappedMemory manager, long value,
			long firstByte) {
		byte[] numberBytes = (byte[]) numberBytesThreadLocal_.get();
		Bytes.longToBytes(value, numberBytes, 0);
		copyToMemory(manager, numberBytes, 0, 8, firstByte);
	}

	/**
	 * Copy an int onto memory. Just patches into the copyToMemory method.
	 */
	public static void intToMemory(IMappedMemory manager, int value,
			long firstByte) {
		byte[] numberBytes = (byte[]) numberBytesThreadLocal_.get();
		Bytes.intToBytes(value, numberBytes, 0);
		copyToMemory(manager, numberBytes, 0, 4, firstByte);
	}

	/**
	 * Read an int from memory. Just patches to the copyFromMemory method
	 */
	public static int intFromMemory(IMappedMemory manager, long firstByte) {
		byte[] numberBytes = (byte[]) numberBytesThreadLocal_.get();
		copyFromMemory(manager, firstByte, numberBytes, 0, 4);
		return Bytes.toInt(numberBytes);
	}

	/**
	 * Read a long from memory. Just patches to the copyFromMemory method
	 */
	public static long longFromMemory(IMappedMemory manager, long firstByte) {
		byte[] numberBytes = (byte[]) numberBytesThreadLocal_.get();
		copyFromMemory(manager, firstByte, numberBytes, 0, 8);
		return Bytes.toLong(numberBytes);
	}
	
	@Override
	public void close() {
		try {
			file_.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new RuntimeException("File could not be closed");
		}
		blocks_ = null;
		dirty_ = null;
		blockAccessEntries_ = null;
		priorityQueue_ = null;
		diskBlockDirectory_ = null;
		directorySearcher_ = null;
	}
}
