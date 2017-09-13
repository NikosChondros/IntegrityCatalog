package edu.stanford.identiscape.skiplists.disk;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import edu.stanford.identiscape.mappedMemory.IMappedMemory;
import edu.stanford.identiscape.mappedMemory.MappedMemory;
import edu.stanford.identiscape.util.Bytes;
import edu.stanford.identiscape.util.DigestAlgorithm;

/**
 * This is an implementation of the authenticated skip list interface, using a
 * memory-mapped file as its back end. It stores a fixed-length sensitive data
 * portion with every element, which is used in the computation of the next
 * authenticator, and a fixed-length non-sensitive data portion. The
 * interpretation of either data portion is application specific. However, the
 * sensitive data portion is included in the hashes as a flat bit string.
 */
public class SkipList {
	// Constants
	// //////////////////////////////////////////////////////////

	/** The size of all labels. 20 bytes for SHA1 */
	public static final int LABELSIZE = 20;

	// Constants: Metadata offsets and lengths
	// //////////////////////////////////////////////////////////

	/** The offset of the sensitive data size */
	private static final int SENSITIVE_OFFSET = 0;

	/** The length of the sensitive data size field (int) */
	private static final int SENSITIVE_LENGTH = 4;

	/** The offset of the insensitive data size */
	private static final int INSENSITIVE_OFFSET = SENSITIVE_OFFSET
			+ SENSITIVE_LENGTH;

	/** The length of the insensitive data size field (int) */
	private static final int INSENSITIVE_LENGTH = 4;

	/** The first data byte */
	private static final long DATA = INSENSITIVE_OFFSET + INSENSITIVE_LENGTH;

	/** The memory manager. */
	private IMappedMemory memoryManager_;

	/** The next open index in the skip list */
	private long next_;

	/** The next open index in memory */
	private long nextPointer_;

	// Contextual information
	// //////////////////////////////////////////////////////////

	/** The size of the sensitive data portion */
	private int sensitive_;

	/** The size of the insensitive data portion */
	private int insensitive_;

	/**
	 * Create a new, empty skip list given an initial value, the sizes of the
	 * sensitive and insensitive data values and a fresh memory manager.
	 * <P>
	 * 
	 * @param initial
	 *            The value of the 0-th index.
	 * @param sensistiveSize
	 *            The byte size of sensitive data values.
	 * @param insensitiveSize
	 *            The byte size of insensitive data values.
	 * @param manager
	 *            The fresh memory manager.
	 */
	public SkipList(byte[] initialBytes, int sensitiveSize,
			int insensitiveSize, IMappedMemory manager) {
		// Sizes must be non-negative. SensitiveSize must be
		// positive.
		if (sensitiveSize <= 0) {
			throw new RuntimeException("At least a single byte "
					+ "of sensitive data is " + "required");
		}
		sensitive_ = sensitiveSize;

		if (insensitiveSize < 0) {
			throw new RuntimeException("Invalid insensitive " + "data size");
		}
		insensitive_ = insensitiveSize;

		// If the memory manager is not fresh, throw an exception
		if (!manager.isFresh()) {
			throw new RuntimeException("Can't create a new "
					+ "skip list on a " + "used memory manager");
		}
		memoryManager_ = manager;

		// Check the initial value
		if (initialBytes == null) {
			throw new NullPointerException("Initial value " + "cannot be null");
		}

		// Store the metadata and the initial authenticator
		intToMemory(sensitive_, SENSITIVE_OFFSET);
		intToMemory(insensitive_, INSENSITIVE_OFFSET);
		copyToMemory(initialBytes, 0, LABELSIZE, DATA);

		// Initialize the next field
		next_ = 1L;

		// And the next pointer (right after the initial value)
		nextPointer_ = DATA + LABELSIZE;
	}

	/**
	 * Create a new skip list from a previously stored copy on a memory manager,
	 * given the next empty index and the manager itself.
	 * 
	 * @param nextIndex
	 *            The value of the next index in the skip list.
	 * @param manager
	 *            The memory manager.
	 */
	public SkipList(long nextIndex, IMappedMemory manager) {
		memoryManager_ = manager;

		// Read in the data sizes
		sensitive_ = intFromMemory(SENSITIVE_OFFSET);
		insensitive_ = intFromMemory(INSENSITIVE_OFFSET);

		// Initialize the next field
		next_ = nextIndex;

		// And the next pointer
		nextPointer_ = DATA + indexToPointer(next_);
	}

	// Implementation of the AuthenticatedSkipList interface
	// //////////////////////////////////////////////////////////

	/**
	 * Append a new element into the skip list. The new element is positioned in
	 * the next empty slot of the skip list. The label of that element in the
	 * skip list is returned.
	 * 
	 * <P>
	 * 
	 * An element consists of sensitive and non-sensitive data. Sensitive data
	 * are included in the calculation of the next authenticator. Non-sensitive
	 * data are just stored along with the element, but do not participate in
	 * the authenticator calculations.
	 * 
	 * <P>
	 * 
	 * @param sensitiveMemory
	 *            The memory buffer holding the sensitive data bytes for the new
	 *            element.
	 * @param sensitiveOffset
	 *            The offset into the sensitive memory buffer where the
	 *            sensitive data bytes are held.
	 * @param riderMemory
	 *            The memory buffer holding the non-sensitive data bytes for the
	 *            new element. May be null, in which case we copy no rider data.
	 * @param riderOffset
	 *            The offset into the rider buffer where the non-sensitive data
	 *            bytes are held.
	 * @return A new byte array holding the authenticator of the skip list after
	 *         the insertion of the new element.
	 */
	public byte[] append(byte[] sensitiveMemory, int sensitiveOffset,
			byte[] riderMemory, int riderOffset) {
		// Lay down the element data, first the sensitive, then the insensitive
		copyToMemory(sensitiveMemory, sensitiveOffset, sensitive_, nextPointer_);
		nextPointer_ += sensitive_;
		if (riderMemory != null) {
			copyToMemory(riderMemory, riderOffset, insensitive_, nextPointer_);
		}
		nextPointer_ += insensitive_;

		// The new node will have extra backward pointers equal to
		// the number of its trailing zeroes.
		int extraPointers = trailingZeroes(next_);

		// Build tower. Starting from level 1 to the number of
		// extra pointers, find where the corresponding label is and
		// calculate the pointer

		// Use the auxilliary instance of the algorithm to calculate
		// the top value
		DigestAlgorithm.ALGORITHM2.reset();
		long offset = 1L;
		byte[] link = null;
		for (int i = 0; i <= extraPointers; i++, offset *= 2L) {
			// The index of the entry after the one with which I
			// will link
			long index = next_ - offset + 1;

			// The starting pointer of the index-th entry
			long memoryPointer = DATA + indexToPointer(index);

			// The actual location of the top label of the
			// (index-1)-th entry
			memoryPointer -= LABELSIZE;

			// The value of the backward pointer
			link = calculatePointer(memoryPointer, i, sensitiveMemory,
					sensitiveOffset, next_);

			// Update the auxilliary algorithm that calculates the
			// top value
			DigestAlgorithm.ALGORITHM2.update(link);

			// Lay down the pointer value
			copyToMemory(link, 0, LABELSIZE, nextPointer_);

			// Advance the memory pointer
			nextPointer_ += LABELSIZE;
		}

		// Calculate the top label if necessary and lay it out
		if (extraPointers > 0) {
			link = DigestAlgorithm.ALGORITHM2.digest();
			copyToMemory(link, 0, LABELSIZE, nextPointer_);
			nextPointer_ += LABELSIZE;
		}

		// Push up the next index
		next_++;

		// And return the top link value
		return link;
	}

	/**
	 * Retrieve a proof of precedence from an earlier element index to a later
	 * element index. If any of the indices are non existent (because the next
	 * open index is less), an exception is thrown. If the first index is
	 * greater than or equal to the second index, an exception is thrown.
	 * 
	 * <P>
	 * 
	 * @param from
	 *            The beginning element index.
	 * @param to
	 *            The target element index.
	 * @return A proof that the from element precedes the to element.
	 */
	public PrecedenceProof proof(long from, long to) {
		// Check the indices for validity
		if (from < 0) {
			throw new RuntimeException("Invalid from (" + from + ")");
		}
		if (from >= to) {
			throw new RuntimeException("\"From\" (" + from
					+ ") must be strictly " + "less than \"to\" (" + to + ")");
		}
		if (to >= next_) {
			throw new RuntimeException("\"To\" (" + to + ") "
					+ "is a non-existent " + "index");
		}

		// Bit position we are considering
		int bitPos = 0;

		// The remaining bits of from
		long fromCurrent = from;

		// The remaining bits of to
		long toCurrent = to;

		// In what bit position was the earliest unit I've seen
		// since the last hop?
		int onePos = -1;

		// Reset the proof helper
		PrecedenceProof.reset();

		// Moving from the to index up to taller towers, up until
		// the tallest tower between from and to is reached While
		// the remaining bits are still different
		while (fromCurrent != toCurrent) {
			// If the first unchecked bit in from is 1
			if ((fromCurrent & 1L) == 1L) {
				// If I haven't seen one yet, record this
				if (onePos == -1) {
					onePos = bitPos;
				}
			} else {
				// If I've seen a one before
				if (onePos != -1) {
					// it's time to climb to the next tower
					long nextIndex = (fromCurrent | 1L) << bitPos;
					commitPointer(nextIndex, onePos);

					// And remember this bit position as the next
					// onePos
					onePos = bitPos;
				}
			}

			// Move to and from right by one bit
			bitPos++;
			fromCurrent = fromCurrent >> 1;
			toCurrent = toCurrent >> 1;
		}

		// If I've seen a unit in the from index, I don't need to
		// emmit the top tower. Either, build it and emmit it.
		fromCurrent = ((fromCurrent << 1) | 1L) << (bitPos - 1);
		if (onePos == -1) {
			long nextIndex = fromCurrent;
			commitPointer(nextIndex, bitPos - 1);
		}

		// Now move down successively shorter towers towards the
		// from index, by adding one unit at a time from its shifted
		// out bits
		for (int i = bitPos - 2; i >= 0; i--) {
			// If there's a unit in the i-th bit position of to
			if ((to & (1L << i)) > 0) {
				// Put it into from Current and emmit it
				fromCurrent = fromCurrent | (1L << i);
				long nextIndex = fromCurrent;
				commitPointer(nextIndex, i);
			}
		}

		// Build the proof
		PrecedenceProof proof = PrecedenceProof.conclude(from, to, sensitive_);

		return proof;
	}

	/** Get the insensitive value of a given element */
	public void insensitive(long index, byte[] buffer, int offset) {
		// Find the start of the element
		long start = DATA + indexToPointer(index);

		// The insensitive data come after the sensitive data
		start += sensitive_;

		copyFromMemory(start, buffer, offset, insensitive_);
	}

	/** Get the sensitive value of a given element */
	public void sensitive(long index, byte[] buffer, int offset) {
		// Find the start of the element
		long start = DATA + indexToPointer(index);

		// The sensitive data come at the beginning of the element.
		copyFromMemory(start, buffer, offset, sensitive_);
	}

	/**
	 * Get the authenticator of a given element into the supplied byte array.
	 */
	public void authenticator(long index, byte[] buffer, int offset) {
		// Find the start of the following element
		long start = DATA + indexToPointer(index + 1);

		// The last LABELSIZE bytes are the authenticator
		start -= LABELSIZE;

		copyFromMemory(start, buffer, offset, LABELSIZE);
	}

	/** Commit a skip list to disk. Flushes out its memory manager */
	public void commit() {
		memoryManager_.flush();
	}

	/** Commit a skip list to disk and reset the memory manager */
	public void commitReset() {
		memoryManager_.flushReset();
	}

	/** Return my size on disk */
	public long size() {
		return memoryManager_.size();
	}

	// Convenience methods: To/From memory
	// //////////////////////////////////////////////////////////

	/**
	 * Copy a portion of a byte array onto memory. The buffer array is assumed
	 * to be large enough for the values used (i.e., it's length is expected to
	 * be at least offset+size).
	 * 
	 * <P>
	 * 
	 * @param buffer
	 *            The buffer from which data are copied out.
	 * @param offset
	 *            The offset into the buffer where the copying starts.
	 * @param size
	 *            The number of bytes to copy.
	 * @param firstByte
	 *            The first byte in the manager to which data bytes are copies.
	 */
	private void copyToMemory(byte[] buffer, int offset, int size,
			long firstByte) {
		// Find the next block to copy into
		long nextBlock = firstByte / IMappedMemory.BLOCKSIZE;
		int relativeFirstByte = (int) (firstByte % IMappedMemory.BLOCKSIZE);
		int chunkSize;
		byte[] block;

		while (size > 0) {
			// Get the next block and soil it
			block = memoryManager_.getBlock(nextBlock);
			memoryManager_.soilBlock(nextBlock);

			if (size + relativeFirstByte > IMappedMemory.BLOCKSIZE) {
				chunkSize = IMappedMemory.BLOCKSIZE - relativeFirstByte;
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
	private void copyFromMemory(long firstByte, byte[] buffer, int offset,
			int size) {
		// Find the block containing the first byte, and the
		// offset of the first byte in that block
		long nextBlock = firstByte / IMappedMemory.BLOCKSIZE;
		int relativeFirstByte = (int) (firstByte % IMappedMemory.BLOCKSIZE);
		byte[] block;
		int chunkSize;

		while (size > 0) {
			// Get the first block and soil it
			block = memoryManager_.getBlock(nextBlock);

			// Feed as much as can be read from the current block
			if (IMappedMemory.BLOCKSIZE > relativeFirstByte + size) {
				chunkSize = size;
			} else {
				chunkSize = IMappedMemory.BLOCKSIZE - relativeFirstByte;
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
	private void writeFromMemory(long firstByte, int size, DataOutput eater) {
		// Find the block containing the first byte, and the
		// offset of the first byte in that block
		long nextBlock = firstByte / IMappedMemory.BLOCKSIZE;
		int relativeFirstByte = (int) (firstByte % IMappedMemory.BLOCKSIZE);
		byte[] block;
		int chunkSize;

		while (size > 0) {
			// Get the first block and soil it
			block = memoryManager_.getBlock(nextBlock);

			// Feed as much as can be read from the current block
			if (IMappedMemory.BLOCKSIZE > relativeFirstByte + size) {
				chunkSize = size;
			} else {
				chunkSize = (int) (IMappedMemory.BLOCKSIZE - relativeFirstByte);
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

	/** Convenience buffer for number to memory copies */
	private static byte[] numberBytes = new byte[8];

	/**
	 * Copy a long onto memory. Just patches into the copyToMemory method.
	 */
	private void longToMemory(long value, long firstByte) {
		Bytes.longToBytes(value, numberBytes, 0);
		copyToMemory(numberBytes, 0, 8, firstByte);
	}

	/**
	 * Copy an int onto memory. Just patches into the copyToMemory method.
	 */
	private void intToMemory(int value, long firstByte) {
		Bytes.intToBytes(value, numberBytes, 0);
		copyToMemory(numberBytes, 0, 4, firstByte);
	}

	/**
	 * Read an int from memory. Just patches to the copyFromMemory method
	 */
	private int intFromMemory(long firstByte) {
		copyFromMemory(firstByte, numberBytes, 0, 4);
		return Bytes.toInt(numberBytes);
	}

	/**
	 * Read a long from memory. Just patches to the copyFromMemory method
	 */
	private long longFromMemory(long firstByte) {
		copyFromMemory(firstByte, numberBytes, 0, 8);
		return Bytes.toLong(numberBytes);
	}

	// Convenience methods: index manipulations
	// //////////////////////////////////////////////////////////

	/**
	 * Calculate the number of trailing binary zeroes of a long number.
	 * 
	 * @param number
	 *            The long number
	 * @return The number of trailing zeroes in the binary representation of the
	 *         long number
	 */
	private int trailingZeroes(long number) {
		int counter = 0;
		long current = number;
		while ((current & 1L) == 0) {
			counter++;
			current = current >> 1;
		}
		return counter;
	}

	/**
	 * Calculate the number of slots in nodes [1..2^(k+1)). Every odd node takes
	 * 2 slots, every other node j*2^i takes i+3 nodes. The method is a solution
	 * of the sumation.
	 * 
	 * @param k
	 *            The height of the complete binary tree in question.
	 * @return The number of slots taken up by all nodes in the tree.
	 */
	private long slotsInCompleteBinaryTree(long k) {
		long value = 1L << k; // This means 2^k
		return 7L * value - k - 5;
	}

	/**
	 * Calculate the starting pointer of an entry given its index.
	 * 
	 * @param index
	 *            The index of the entry, starting from 0
	 * @return The starting pointer of the entry in memory. This is an offset
	 *         into the DATA portion of the memory.
	 */
	private long indexToPointer(long index) {
		// The 0-th slot begins at the beginning of data.
		if (index == 0L) {
			return 0L;
		}

		long slots = 1L; // Taken by the 0th node
		int bitPos = 0; // The bit position we're considering
		boolean seenOne = ((index & 1L) == 1L); // Have we seen a
		// unit so far?
		long current = index >> 1;// All bits but the least
		// significant

		// While we still have units left in the binary
		// representation of index
		while (current > 0L) {
			// If the least significant bit is a unit
			if ((current & 1L) == 1L) {
				slots += slotsInCompleteBinaryTree(bitPos);
				if (seenOne) {
					slots += bitPos + 4; // The number of slots in a
					// tower of bitPos additional
					// pointers
				}
				seenOne = true; // We just saw a unit, so update
				// this guy
			}

			// Increment the bit position
			bitPos++;

			// Remove yet another bit
			current = current >> 1;
		}

		return (slots - index + 1) * LABELSIZE + // slots for links
				(index - 1) * (sensitive_ + insensitive_); // one per
		// data element
	}

	// Convenience methods: Hash manipulations
	// //////////////////////////////////////////////////////////

	/**
	 * Calculate the value of a pointer.
	 * 
	 * @param source
	 *            The offset of the source pointer for this link.
	 * @param level
	 *            The level of the link.
	 * @param datumMemory
	 *            The memory buffer holding the (sensitive) datum of the
	 *            destination.
	 * @param datumStart
	 *            The offset of the (sensitive) datum bytes in the memory buffer
	 * @param index
	 *            The index of the destination.
	 * @return A new byte array holding the value of the pointer (a label).
	 */
	private byte[] calculatePointer(long source, int level, byte[] datumMemory,
			int datumStart, long index) {
		// Submit the source, the level, the datum and the index.
		// Return the result.
		DigestAlgorithm.ALGORITHM.reset();

		// Source label. Write into the digest byte eater the
		// source label.
		writeFromMemory(source, LABELSIZE, DigestAlgorithm.EATER);

		// The level of the link
		DigestAlgorithm.ALGORITHM.update(Bytes.intToBytesInPlace(level));

		// The sensitive data of the destination
		DigestAlgorithm.ALGORITHM.update(datumMemory, datumStart, sensitive_);

		// The index of the destination
		DigestAlgorithm.ALGORITHM.update(Bytes.longToBytesInPlace(index));

		return DigestAlgorithm.ALGORITHM.digest();
	}

	/**
	 * Commits a link pointer of an index at a given level to the precedence
	 * proof helper
	 * 
	 * @param index
	 *            The index of the node whose link we commit.
	 * @param level
	 *            The level of the link at that index.
	 */
	private void commitPointer(long index, int level) {
		// Find the start of the index-th node
		long memoryPointer = DATA + indexToPointer(index);

		// Find the number of extra links in the node
		int extraLinks = trailingZeroes(index);

		// If the requested level is greater than the number of
		// extra links, then throw an exception
		if (level > extraLinks) {
			throw new RuntimeException("Invalid requested level " + level
					+ " from index " + Long.toBinaryString(index));
		}

		// Commit the sensitive data of the node
		writeFromMemory(memoryPointer, sensitive_, PrecedenceProof.EATER);

		// Skip the data (sensitive and insensitive)
		memoryPointer += sensitive_ + insensitive_;

		// Commit all of the link values except for the one of the
		// requested level
		if (level > 0) {
			writeFromMemory(memoryPointer, LABELSIZE * level,
					PrecedenceProof.EATER);
			memoryPointer += LABELSIZE * level;
		}
		// Skip over requested level
		memoryPointer += LABELSIZE;

		// And what follows the requested level
		if (extraLinks > level) {
			writeFromMemory(memoryPointer, LABELSIZE * (extraLinks - level),
					PrecedenceProof.EATER);
		}
	}
	
	/**
	 * close the skip list file and release all memory
	 */
	public void close() {
		memoryManager_.close();
		memoryManager_ = null;
	}

	// Testing functionality
	// //////////////////////////////////////////////////////////

	/** An all-zero label */
	public static final byte[] NILLABEL = new byte[LABELSIZE];
	static {
		Arrays.fill(NILLABEL, (byte) 0);
	}

	/** The method used to create and measure skip list appends */
	public static void appends(int all, int snapshot) {
		// Construct an array to store digests in
		int allDigests = all;

		// Create a fresh memory manager, with a 100-block Java
		// cache
		IMappedMemory manager = new MappedMemory("timeweaveData/SkipList.dat",
				100);

		// Create the skip list from scratch. Sensitive data are
		// SHA1 labels and there are no insensitive data.
		SkipList skipList = new SkipList(NILLABEL, 20, 20, manager);

		Random random = new Random(0);
		byte[] value = new byte[20];
		long beforeCommit;
		long afterCommit;
		long commits = 0L;
		// Build a bunch of values and stick them in
		System.out.println(all + "/" + snapshot);
		System.out.println(System.currentTimeMillis());
		for (long i = 1; i < allDigests; i++) {

			random.nextBytes(value);
			skipList.append(value, 0, value, 0);
			beforeCommit = System.currentTimeMillis();
			skipList.commitReset();
			afterCommit = System.currentTimeMillis();
			commits += afterCommit - beforeCommit;
			if (i % snapshot == 0) {
				System.out.println(i + " T " + System.currentTimeMillis()
						+ " C " + commits);
			}
		}

		// Commit the list to disk
		// skipList.commit();

		System.out.println(allDigests + " T " + System.currentTimeMillis()
				+ " C " + commits);
	}

	public static void existence(int digests, int pairs) {
		// Construct an array to store digests in
		int allDigests = digests;
		int allPairs = pairs;
		int last = digests - 1;

		// Create a memory manager from an existing file, with a 1GB
		// cache (65536 x 16KByte blocks)
		IMappedMemory manager = new MappedMemory("timeweaveData/SkipList.dat",
				0x8000);

		// Create the existing skip list on this manager.
		SkipList skipList = new SkipList(allDigests, manager);

		// Then retrieve random pairs of values, retrieve their
		// precedence proofs from the skip lists and validate them.

		System.out.print(" E " + System.currentTimeMillis());
		Random random = new Random(0);
		for (int i = 0; i < allPairs; i++) {
			long index = random.nextLong() % last;

			// Get a proof of the pair
			PrecedenceProof proof = skipList.proof(index, last);
		}
		System.out.print(" " + System.currentTimeMillis());
	}

	public static void threadExistence(int digests, int trials, int distance) {
		// Construct an array to store digests in
		int last = digests - 1;

		// Create a memory manager from an existing file, with a 1GB
		// cache (65536 x 16KByte blocks)
		IMappedMemory manager = new MappedMemory("timeweaveData/SkipList.dat",
				0x8000);

		// Create the existing skip list on this manager.
		SkipList skipList = new SkipList(digests, manager);

		// Then retrieve random precedences from within the distance
		// to the last one
		System.out.print(" E " + System.currentTimeMillis());
		Random random = new Random(0);
		for (int i = 0; i < trials; i++) {
			long index = last - random.nextInt(distance) - 1;

			// Get a proof of the pair
			skipList.proof(index, last);
		}
		System.out.print(" " + System.currentTimeMillis());
	}

	public static void precedence(int digests, int pairs) {
		// Construct an array to store digests in
		int allDigests = digests;
		int allPairs = pairs;
		int last = digests - 1;

		// Create a memory manager from an existing file, with a 1GB
		// cache (65536 x 16KByte blocks)
		IMappedMemory manager = new MappedMemory("timeweaveData/SkipList.dat",
				0x8000);

		// Create the existing skip list on this manager.
		SkipList skipList = new SkipList(allDigests, manager);

		// Then retrieve random pairs of values, retrieve their
		// precedence proofs from the skip lists and validate them.

		System.out.print(" P " + System.currentTimeMillis());
		Random random = new Random(0);
		for (int i = 0; i < allPairs; i++) {
			long first = random.nextInt(last);
			long second = random.nextInt(last);
			if (first < second) {
				// Get a proof of the pair
				skipList.proof(first, second);
			} else if (second < first) {
				skipList.proof(second, first);
			} else {
				i--;
			}
		}
		System.out.print(" " + System.currentTimeMillis());
	}

	/** Run the searches experiment */
	public static void searches(int total, int pairs) {
		int increment = 1000000;
		int count = 29000000;
		int flushsize = 1700000000;
		{
			byte[] b = new byte[flushsize];
			b = null;
		}
		System.gc();
		System.runFinalization();
		while (count <= total) {
			System.out.print(count + " " + System.currentTimeMillis());
			existence(count, pairs);
			System.gc();
			System.runFinalization();
			{
				byte[] b = new byte[flushsize];
				b = null;
			}
			System.gc();
			System.runFinalization();
			precedence(count, pairs);
			System.gc();
			System.runFinalization();
			{
				byte[] b = new byte[flushsize];
				b = null;
			}
			System.gc();
			System.runFinalization();
			count += increment;
			System.out.println("");
		}
	}

	/** Run the searches experiment */
	public static void threadSearches(String[] args) {
		int minDistance = Integer.parseInt(args[0]);
		int increment = Integer.parseInt(args[1]);
		int maxDistance = Integer.parseInt(args[2]);
		int trials = Integer.parseInt(args[3]);
		int total = Integer.parseInt(args[4]);

		int flushsize = 1700000000;
		{
			byte[] b = new byte[flushsize];
			b = null;
		}
		System.gc();
		System.runFinalization();
		for (int i = minDistance; i <= maxDistance; i += increment) {
			System.out.print(i + " " + System.currentTimeMillis());
			threadExistence(total, trials, i);
			System.gc();
			System.runFinalization();
			{
				byte[] b = new byte[flushsize];
				b = null;
			}
			System.gc();
			System.runFinalization();
			System.out.println("");
		}
	}

	public static void wipe() {
		int flushsize = 1700000000;
		System.gc();
		System.runFinalization();
		{
			byte[] b = new byte[flushsize];
			b = null;
		}
		System.gc();
		System.runFinalization();
	}

	/** Verification experiment */
	public static void verifications(int size, int pairs, int distances) {
		// Construct an array to store digests in
		int allDigests = size;
		int allPairs = pairs;
		int allDistances = distances;
		int last = allDigests - 1;
		PrecedenceProof[] proofs = new PrecedenceProof[pairs];
		byte[][] fromAuths = new byte[pairs][LABELSIZE];
		byte[][] toAuths = new byte[pairs][LABELSIZE];

		// Create a memory manager from an existing file, with a 512MB
		// cache (32K x 16KByte blocks)
		IMappedMemory manager = new MappedMemory("timeweaveData/SkipList.dat",
				100);

		// Create the existing skip list on this manager.
		SkipList skipList = new SkipList(allDigests, manager);

		// Then for all distances
		for (int d = 10; d < allDigests - 10; d += (allDigests - 20)
				/ distances) {
			// Then retrieve random pairs of values, retrieve
			// their precedence proofs from the skip lists and
			// validate them.
			Random random = new Random(0);
			System.out.print(allDigests + " " + allPairs + " " + d + " "
					+ System.currentTimeMillis());
			for (int i = 0; i < allPairs; i++) {
				long first = random.nextInt(last - d);
				long second = first + d;
				proofs[i] = skipList.proof(first, second);
				// skipList.authenticator(first, fromAuths[i], 0);
				// skipList.authenticator(second, toAuths[i], 0);
			}
			System.out.print(" " + System.currentTimeMillis());

			// And now verify them all, and count the average proof
			// size in bytes
			long proofSize = 0;
			long minSize = Long.MAX_VALUE;
			long maxSize = 0;
			long thisSize;
			// System.out.print(" P " + System.currentTimeMillis());
			for (int i = 0; i < allPairs; i++) {
				// if (!proofs[i].validate(fromAuths[i], toAuths[i],
				// 20)) { // 20 sensitive bytes
				// System.out.println("\ndoing!");
				// }
				thisSize = proofs[i].size();
				if (maxSize < thisSize) {
					maxSize = thisSize;
				}
				if (minSize > thisSize) {
					minSize = thisSize;
				}
				proofSize += proofs[i].size();
			}
			// System.out.println(" " + System.currentTimeMillis() +
			// " S " + minSize + " " + proofSize +
			// " " + maxSize);
			System.out.println(" " + minSize + " " + proofSize / allPairs + " "
					+ maxSize);
		}
	}

	/** A comprehensive experiment for skip lists */
	public static void comprehensive(int totalSize, int sizeIncrement,
			int trials) {
		String sFile = "timeweaveData/SkipList.dat";
		(new File(sFile)).delete();

		// The skip list manager
		IMappedMemory skipMemoryManager = new MappedMemory(sFile, 100);

		// Start up the skip list
		SkipList skiplist = new SkipList(NILLABEL, 20, // SHA1 labels as
				// sensitive data
				0,// no insensitive data
				skipMemoryManager);

		byte[] value = new byte[20];
		Random rand = new Random(0);
		long index;
		long skip;
		System.out.println(totalSize + "/" + sizeIncrement + "/" + trials);
		System.out.println(System.currentTimeMillis());
		for (int size = sizeIncrement; size < totalSize + sizeIncrement; size += sizeIncrement) {
			int pMinSize = Integer.MAX_VALUE;
			int pMaxSize = 0;
			int pTotalSize = 0;
			int pSize;
			System.out.print(size + " " + System.currentTimeMillis());
			for (int i = 0; i < sizeIncrement; i++) {
				rand.nextBytes(value);
				skiplist.append(value, 0, (byte[]) null, 0);
			}
			skiplist.commit();
			System.out.print(" A " + System.currentTimeMillis());

			skip = size / trials;
			for (int i = 0; i < trials; i++) {
				index = skip * i;
				PrecedenceProof proof = skiplist.proof(index, size);
				pSize = proof.size();
				if (pSize < pMinSize) {
					pMinSize = pSize;
				}
				if (pSize > pMaxSize) {
					pMaxSize = pSize;
				}
				pTotalSize += pSize;
			}

			System.out.print(" S " + System.currentTimeMillis());

			System.out.println(" PS " + pMinSize + " "
					+ (((double) pTotalSize) / trials) + " " + pMaxSize
					+ " FS " + skipMemoryManager.size());
		}
	}

	public static void main(String[] args) {
		appends(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
		// comprehensive(Integer.parseInt(args[0]),
		// Integer.parseInt(args[1]),
		// Integer.parseInt(args[2]));
		// verifications(Integer.parseInt(args[0]),
		// Integer.parseInt(args[1]),
		// Integer.parseInt(args[2]));
	}
}
