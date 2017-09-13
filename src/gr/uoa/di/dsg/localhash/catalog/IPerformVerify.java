package gr.uoa.di.dsg.localhash.catalog;

import java.util.TreeMap;

public interface IPerformVerify {

	public abstract boolean verify(
			TreeMap<Long, TreeMap<ByteArrayWrapper, Integer>> votes,
			int voteCount, SnapshotData currentSD);

}