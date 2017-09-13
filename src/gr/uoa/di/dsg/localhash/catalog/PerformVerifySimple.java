package gr.uoa.di.dsg.localhash.catalog;

import java.util.TreeMap;

public class PerformVerifySimple implements IPerformVerify {

	/* (non-Javadoc)
	 * @see gr.uoa.di.dsg.localhash.catalog.IPerformVerify#verify(java.util.TreeMap, int, gr.uoa.di.dsg.localhash.catalog.SnapshotData)
	 */
	@Override
	public boolean verify(TreeMap<Long, TreeMap<ByteArrayWrapper, Integer>> votes, int voteCount, SnapshotData currentSD) {
		boolean ret = false;
		
		//how many votes did my version get?
		int numVotes = 0;
		TreeMap<ByteArrayWrapper, Integer> versionVotes =  votes.get(currentSD.snapshotId);
		if( versionVotes != null ) {
			Integer myVotes = versionVotes.get(new ByteArrayWrapper(currentSD.authenticator));
			if( myVotes != null )
				numVotes = myVotes;
		}
		//confirm when votes are greater than 20% of the votes (but at least 2)
		int minimumVotes = Math.max(voteCount / 5, 2);
		if( numVotes >= minimumVotes )
			ret = true;
		return ret;
	}
}
