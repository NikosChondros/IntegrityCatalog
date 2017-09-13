package edu.stanford.identiscape.skiplists.disk;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import edu.stanford.identiscape.util.ByteArrayRegion;
import edu.stanford.identiscape.util.ByteEater;
import edu.stanford.identiscape.util.Bytes;
import edu.stanford.identiscape.util.DigestAlgorithm;

/**
 * This is a precedence proof of two elements in an authenticated skip list.
 */
public class PrecedenceProof implements Serializable {
	/** The size of all labels. 20 bytes for SHA1 */
	public static final int LABELSIZE = 20;

	/** The raw bytes of the precedence proof */
	private byte[] bytes_;

	/** The source index */
	private long from_;

	/** The destination index */
	private long to_;

	/** The size of the sensitive portion of the data in the proof */
	private int sensitive_;

	/** My size_ */
	private int size_;

	/**
	 * Construct a precedence proof
	 * 
	 * @param from
	 *            The source index.
	 * @param to
	 *            The destination index.
	 * @param bytes
	 *            The byte array carrying the proof.
	 */
	public PrecedenceProof(long from, long to, byte[] bytes, int sensitive) {
		// Check the from/to parameters
		if ((from < 0) || (to < 0)) {
			throw new RuntimeException("Invalid from/to indices");
		}
		if (from >= to) {
			throw new RuntimeException("A precedence proof "
					+ "requires a non-empty " + "interval between source "
					+ "and destination");
		}

		// Check that the byte array is not null
		if (bytes == null) {
			throw new NullPointerException("A proof requires " + "a non-null "
					+ "byte array");
		}

		// Store everything
		from_ = from;
		to_ = to;
		bytes_ = bytes;
		sensitive_ = sensitive;
		size_ = 8 + // from
		8 + // to
		4 + // array length
		bytes_.length; // actual bytes
	}

	/** Construct a new precedence proof from a byte spewer. */
	public PrecedenceProof(DataInput spewer) {
		try {
			// First read in the source and destination indices
			from_ = spewer.readLong();
			// Check the from parameter
			if (from_ < 0) {
				throw new RuntimeException("Invalid from index " + from_);
			}

			to_ = spewer.readLong();
			// Check the to parameter and its relationship with the
			// from parameter.
			if ((to_ < 0) || (to_ < from_)) {
				throw new RuntimeException("Invalid to index " + to_);
			}

			// Then the size of the proof bytes
			int length = spewer.readInt();
			if (length < 0) {
				throw new RuntimeException("Invalid byte " + "array length "
						+ length);
			}
			bytes_ = new byte[length];
			spewer.readFully(bytes_);

			size_ = 8 + // from
			8 + // to
			4 + // array length
			bytes_.length; // bytes
		} catch (IOException ioe) {
			throw new RuntimeException("Could not fully read "
					+ "a precedence proof from " + "the byte spewer");
		}
	}

	/** Get the value from which the proof begins */
	public long from() {
		return from_;
	}

	/** Get the value at which the proof ends */
	public long to() {
		return to_;
	}

	/**
	 * Return the size of the proof in bytes. This is the sum of the proof
	 * buffer, and the source and target index numbers.
	 */
	public int size() {
		return size_;
	}

	/**
	 * A convenience instance of MungeResult for interactions with mungePointer
	 */
	private static MungeResult result = new MungeResult();

	/**
	 * Validate the proof, given the source and target authenticators.
	 * 
	 * @param sourceAuthenticator
	 *            The authenticator of the source of the proof
	 * @param targetAuthenticator
	 *            The authenticator of the target of the proof.
	 * @param sensitiveSize
	 *            The size of sensitive bytes at every element.
	 * @return True if the validation succeeds, false otherwise.
	 */
	public boolean validate(byte[] sourceAuthenticator,
			byte[] targetAuthenticator) {
		int sensitiveSize = sensitive_; //Instead of parameter, use the one in the instance
		// Start from the beginning of the proof
		result.pointer = 0;

		// Start with the source authenticator
		byte[] currentValue = sourceAuthenticator;

		// Bit position we are considering
		int bitPos = 0;

		// The remaining bits of from
		long fromCurrent = from_;

		// The remaining bits of to
		long toCurrent = to_;

		// In what bit position was the earliest unit I've seen
		// since the last hop?
		int onePos = -1;

		// Moving from the to index up to taller towers, up until
		// the tallest tower between from and to is reached

		// While the remaining bits are still different
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

					// Munge this link from the proof
					currentValue = mungeLink(currentValue, nextIndex, onePos,
							result, sensitiveSize);

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

		// If I've seen a unit in the from index, I don't need to emmit
		// the top tower. Otherwise, build it and emmit it.
		fromCurrent = ((fromCurrent << 1) | 1L) << (bitPos - 1);
		if (onePos == -1) {
			long nextIndex = fromCurrent;
			currentValue = mungeLink(currentValue, nextIndex, bitPos - 1,
					result, sensitiveSize);
		}

		// Now move down successively shorter towers towards the
		// from index, by adding one unit at a time from its shifted
		// out bits
		for (int i = bitPos - 2; i >= 0; i--) {
			// If there's a unit in the i-th bit position of to
			if ((to_ & (1L << i)) > 0) {
				// Put it into from Current and emmit it
				fromCurrent = fromCurrent | (1L << i);
				long nextIndex = fromCurrent;
				currentValue = mungeLink(currentValue, nextIndex, i, result,
						sensitiveSize);
			}
		}

		// Now compare the two values: the calculated and the target
		// authenticator
		// System.out.println("");
		return Arrays.equals(targetAuthenticator, currentValue);
	}

	/**
	 * Validate the last-hop sensitive datum. Compare the two and return true if
	 * they match, false otherwise
	 */
	public boolean validateLastHop(byte[] sensitive) {
		// Get the extra links of the last hop
		int extraLinks = trailingZeroes(to_);

		// If there are extra links, then skip them backwards from the end
		// of the proof
		int pointer = bytes_.length;
		if (extraLinks > 0) {
			pointer -= LABELSIZE * extraLinks;
		} else {
			// Otherwise, just start from the end of the proof
		}

		// The datum ends at the current pointer
		pointer -= sensitive.length;

		return Bytes.areEqual(sensitive, 0, bytes_, pointer, sensitive.length);
	}

	/**
	 * Retrieve the payload of the last hop (its sensitive datum). 
	 */
	public ByteArrayRegion getPayloadOfLastHop() {
		// Get the extra links of the last hop
		int extraLinks = trailingZeroes(to_);

		// If there are extra links, then skip them backwards from the end
		// of the proof
		int pointer = bytes_.length;
		if (extraLinks > 0) {
			pointer -= LABELSIZE * extraLinks;
		} else {
			// Otherwise, just start from the end of the proof
		}

		// The datum ends at the current pointer
		pointer -= sensitive_;

		return new ByteArrayRegion(bytes_, pointer, sensitive_);
	}

	/**
	 * Write out a precedence proof into a byte stream.
	 * 
	 * <P>
	 * 
	 * @param DataOutput
	 *            The byte eater into which we are writing this proof.
	 * @return The number of bytes written.
	 */
	public int write(DataOutput eater) throws IOException {
		// First write out the source and destination indices
		eater.writeLong(from_);
		eater.writeLong(to_);

		// Then the size of the proof bytes
		eater.writeInt(bytes_.length);

		// And then all the proof bytes
		eater.write(bytes_);

		// Wrote 20 (for the longs and the array size) + the
		// length of the proof bytes
		return size_;
	}

	// Convenience methods for proof validation
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
	 * Munge proof link. Apply the given link of the proof to the input value to
	 * the proof linik and return the result.
	 * 
	 * @param initialValue
	 *            The input value to the proof link.
	 * @param index
	 *            The index of the next proof link.
	 * @param level
	 *            The level of the next proof link.
	 * @param result
	 *            The return parameter containing the current pointer value
	 *            within the byte buffer.
	 * @param sensitive
	 *            The size of the sensitive data in every skip list element.
	 * @return The result of the computation.
	 */
	private byte[] mungeLink(byte[] initialValue, long index, int level,
			MungeResult result, int sensitive) {
		DigestAlgorithm.ALGORITHM.reset();

		// The initial authenticator
		DigestAlgorithm.ALGORITHM.update(initialValue);

		// The level of the link
		DigestAlgorithm.ALGORITHM.update(Bytes.intToBytesInPlace(level));

		// The data of the destination
		DigestAlgorithm.ALGORITHM.update(bytes_, result.pointer, sensitive);
		result.pointer += sensitive;

		// The index of the destination
		DigestAlgorithm.ALGORITHM.update(Bytes.longToBytesInPlace(index));
		byte[] levelLink = DigestAlgorithm.ALGORITHM.digest();

		// Now put together all level links to produce the super
		// link, if applicable. Otherwise return the result already
		// gotten.

		// The number of extra links in the node
		int extraLinks = trailingZeroes(index);

		if (extraLinks > 0) {
			DigestAlgorithm.ALGORITHM.reset();

			for (int i = 0; i <= extraLinks; i++) {
				// For all levels but the link level, push the link value
				// into the hash function. When the time comes for the
				// link level, push the value we calculated above.
				if (i != level) {
					DigestAlgorithm.ALGORITHM.update(bytes_, result.pointer,
							LABELSIZE);
					result.pointer += LABELSIZE;
				} else {
					DigestAlgorithm.ALGORITHM.update(levelLink);
				}
			}

			byte[] digest = DigestAlgorithm.ALGORITHM.digest();
			return digest;
		} else {
			return levelLink;
		}
	}

	/** Turn to string */
	public String toString() {
		return "<PProof " + from_ + "-->" + to_ + ">";
	}

	// //////////////////////////////////////////////////////////

	/** A record class used for results of mungeLink */
	private static class MungeResult {
		int pointer;
	}

	// Convenience methods for proof construction
	// //////////////////////////////////////////////////////////

	/**
	 * A convenience byte array output buffer for incremental proof
	 * construction.
	 */
	private static ByteArrayOutputStream buffer_ = new ByteArrayOutputStream();

	/** Reset the helper */
	public static void reset() {
		buffer_.reset();
	}

	/** Feed the helper with another byte array */
	public static void feed(byte[] bytes, int start, int size) {
		buffer_.write(bytes, start, size);
	}

	/**
	 * Construct the proof given the source and destination indices and the
	 * current bytes in the helper
	 */
	public static PrecedenceProof conclude(long from, long to, int sensitive) {
		// Construct the new proof
		byte[] proofArray = buffer_.toByteArray();
		PrecedenceProof proof = new PrecedenceProof(from, to, proofArray, sensitive);
		return proof;
	}

	/**
	 * A byte eater into the existence proof helper. It only accepts writes of
	 * byte arrays.
	 */
	private static class HelperByteEater extends ByteEater {
		/** From ByteEater */
		public void write(byte[] b, int off, int len) throws IOException {
			PrecedenceProof.feed(b, off, len);
		}
	}

	/** The byte eater for the helper */
	public static final DataOutput EATER = new HelperByteEater();
}
