package gr.uoa.di.dsg.localhash.catalog;

import java.util.TreeMap;

public interface IChooseRecoverVersion {

	public abstract SnapshotData choose(
			TreeMap<Long, TreeMap<ByteArrayWrapper, Integer>> votes,
			int totalVotes);

}