package edu.stanford.identiscape.mappedMemory;

import java.util.Comparator;

/**
 * This is a record-keeping entry containing the number of a block and the last
 * logical time at which this block was requested. Block access entries are
 * naturally ordered by access time.
 */
class BlockAccessEntry {
	/** My access time comparator */
	static AccessComparator ACCESSCOMPARATOR = new AccessComparator();

	/**
	 * The buffer block to which this entry corresponds. This should never be
	 * changed.
	 */
	private int bufferBlockNumber_;

	/** The disk block to which this buffer block points */
	long diskBlockNumber;

	/** The last logical time this block was requested */
	long accessTime;

	/** Create a new entry */
	BlockAccessEntry(int number, long time) {
		bufferBlockNumber_ = number;
		diskBlockNumber = -1; // initially pointing nowhere
		accessTime = time;
	}

	/** What's my buffer block number? */
	public int getBlockNumber() {
		return bufferBlockNumber_;
	}

	/**
	 * The natural order of block access entries. They are ordered by disk
	 * block.
	 */
	public int compareTo(Object o) {
		BlockAccessEntry other = (BlockAccessEntry) o;
		if (diskBlockNumber < other.diskBlockNumber) {
			return -1;
		} else if (diskBlockNumber > other.diskBlockNumber) {
			return 1;
		} else {
			return 0;
		}
	}

	/** Is this equal to the given? */
	public boolean equals(Object o) {
		if (!(o instanceof BlockAccessEntry)) {
			return false;
		}

		return (compareTo(o) == 0);
	}

	/** The hash code should be compatible with equals */
	public int hashCode() {
		return (int) diskBlockNumber;
	}

	/** A comparator ordering entries by their access times */
	private static class AccessComparator implements Comparator {
		/** Compare two block entries, based on their access times */
		public int compare(Object one, Object two) {
			BlockAccessEntry first = (BlockAccessEntry) one;
			BlockAccessEntry second = (BlockAccessEntry) two;
			if (first.accessTime < second.accessTime) {
				return -1;
			} else if (first.accessTime > second.accessTime) {
				return 1;
			} else {
				return 0;
			}
		}
	}
}
