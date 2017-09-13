package gr.uoa.di.dsg.localhash.catalog;

import java.util.TreeMap;

public class ChooseRecoverVersionSimple implements IChooseRecoverVersion {

	/* (non-Javadoc)
	 * @see gr.uoa.di.dsg.localhash.catalog.IChooseRecoverVersion#choose(java.util.TreeMap, int)
	 */
	@Override
	public SnapshotData choose(TreeMap<Long, TreeMap<ByteArrayWrapper, Integer>> votes, int totalVotes) {
		SnapshotData sd = null;
		
		//chose the latest version with greater than 20% of the votes (but at least 2)
		int minimumVotes = Math.max(totalVotes / 5, 2);
		
		for( Long snapshotId : votes.keySet() ) {
			TreeMap<ByteArrayWrapper, Integer> authVotes = votes.get(snapshotId);
			for( ByteArrayWrapper vauthenticator :  authVotes.keySet() ) {
				int numVotes = authVotes.get(vauthenticator);
				if( numVotes >= minimumVotes ) {
					sd = new SnapshotData(snapshotId, vauthenticator.get());
				}
			}
		}
		if( sd != null )
			return sd;
		
		minimumVotes = 2;
		//failing that, choose the latest one with > 1 votes 
		for( Long snapshotId : votes.keySet() ) {
			TreeMap<ByteArrayWrapper, Integer> authVotes = votes.get(snapshotId);
			for( ByteArrayWrapper vauthenticator :  authVotes.keySet() ) {
				int numVotes = authVotes.get(vauthenticator);
				if( numVotes >= minimumVotes ) {
					sd = new SnapshotData(snapshotId, vauthenticator.get());
				}
			}
		}
		
		return sd;
	}

}
