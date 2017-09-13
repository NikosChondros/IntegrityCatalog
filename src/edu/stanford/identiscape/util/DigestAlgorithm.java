package edu.stanford.identiscape.util;

import java.io.DataOutput;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * This is a convenience function encapsulating the functionality of retrieving
 * the message digest algorithm. It just gets an instance of the particular
 * digest algorithm and keeps it around for users
 */
public class DigestAlgorithm {
	/** The message digest algorithm used by everyone */
	private static final String DIGEST_ALGORITHM_NAME = "SHA-1";
	public static final MessageDigest ALGORITHM;

	/**
	 * The auxilliary instance of the algorithm for concurrent computations
	 */
	public static final MessageDigest ALGORITHM2;

	/** Initialize the message digest algorithm */
	static {
		// Main
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance(DIGEST_ALGORITHM_NAME);
		} catch (NoSuchAlgorithmException nsae) {
			System.err.println("Couldn't initialize " + DIGEST_ALGORITHM_NAME);
			System.exit(-1);
		}
		ALGORITHM = md;

		// Auxilliary
		try {
			md = MessageDigest.getInstance(DIGEST_ALGORITHM_NAME);
		} catch (NoSuchAlgorithmException nsae) {
			System.err.println("Couldn't initialize " + DIGEST_ALGORITHM_NAME);
			System.exit(-1);
		}
		ALGORITHM2 = md;
	}

	/** Don't create instances of this object */
	private DigestAlgorithm() {
	}

	/**
	 * A byte eater into a message digest. It only accepts writes of byte
	 * arrays.
	 */
	private static class DigestByteEater extends ByteEater {
		/** My message digest machine */
		private MessageDigest md_;

		/** Create a digest byte eater given its message digest */
		private DigestByteEater(MessageDigest md) {
			md_ = md;
		}

		/** From ByteEater */
		public void write(byte[] b, int off, int len) throws IOException {
			md_.update(b, off, len);
		}
	}

	/** The byte eater for the first message digest algorithm */
	public static final DataOutput EATER = new DigestByteEater(ALGORITHM);

	/** The byte eater for the second message digest algorithm */
	public static final DataOutput EATER2 = new DigestByteEater(ALGORITHM2);

}
