package gr.uoa.di.dsg.treap;

import java.io.IOException;

import edu.stanford.identiscape.util.Bytes;

public class AuthenticatorCacheManagerDepthBased extends
		AuthenticatorCacheManager {
	// Format is: snapshotIdFrom, snapshotIdTo, Authenticator
	// snapshotIdTo may be Node.OPEN_VALIDITY, implying it is still valid
	private static final int ENTRY_SIZE = 2 * Node.SIZE_OF_SNAPSHOT_ID + Node.SIZE_OF_AUTHENTICATOR_DIGEST;

	private final int every;

	public AuthenticatorCacheManagerDepthBased(int every) {
		this.every = every;
	}

	@Override
	public boolean always() {
		return false;
	}

	@Override
	public int entrySize() {
		return ENTRY_SIZE;
	}

	@Override
	public byte[] getCurrentAuthenticator(Node node, TreePath path)
			throws IOException {
		long snapshotId = node.tree.getCurrentSnapshotId();
		int ca = node.getCountAuthenticator();
		if (ca > 0) {
			int pos = node.getFirstAuthenticatorPos();
			long validUntil = node.backingBuffer.getLong(pos
					+ Node.SIZE_OF_SNAPSHOT_ID);
			if (validUntil == Node.OPEN_VALIDITY || validUntil >= snapshotId) {
				byte[] auth = new byte[Node.SIZE_OF_AUTHENTICATOR_DIGEST];
				node.backingBuffer.get(pos + 2 * Node.SIZE_OF_SNAPSHOT_ID, auth);
				return auth;
			}
		}
		// now, if we reached this spot this means we need to recalculate the darn thing...
		return node.produceCurrentAuthenticator(path, (n) -> {});
	}

	/**
	 * 
	 * @param snapshotId
	 * @param depth
	 * @return Decision (boolean) on weather to store the authenticator or not
	 * 
	 *         It calculates the snapshot id (minus 1 as it is 1 based) mod
	 *         "every" Returns true if depth mode every equals above
	 */
	private final boolean shouldCache(long snapshotId, int depth) {
		int i = (int) ((snapshotId - 1L) % every);
		return depth % every == i;
	}

	@Override
	public void cacheAuthenticator(Node node, long snapshotId, byte[] auth, TreePath path) throws IOException {
		// Should we cache?
		boolean store = shouldCache(snapshotId, path.depth());
		int pos = node.getFirstAuthenticatorPos();
		int ca = node.getCountAuthenticator();
		if (store) {
			// is the last authenticator still valid? If yes, invalidate
			if (ca > 0)
				if (node.backingBuffer.getLong(pos + Node.SIZE_OF_SNAPSHOT_ID) == Node.OPEN_VALIDITY)
					node.backingBuffer.putLong(pos + Node.SIZE_OF_SNAPSHOT_ID,
							snapshotId - 1L);
			// store the new one
			Node targetNode;
			boolean copied = false;
			if (node.canExpandBy(ENTRY_SIZE))
				targetNode = node;
			else {
				long newNodeId = node.copyNode(node.getLeft(), node.getRight(), node.getVersionedValue(), path); //and old 'node' is cleansed
				targetNode = node.tree.fetchNode(newNodeId);
				copied = true;
				pos = targetNode.getFirstAuthenticatorPos();
				ca = targetNode.getCountAuthenticator();
			}
			// we need to insert a new entry
			byte[] extra = new byte[ENTRY_SIZE];
			targetNode.backingBuffer.insert(pos, extra);
			targetNode.setCountAuthenticator(ca + 1);
			targetNode.backingBuffer.putLong(pos, snapshotId); // valid from this snapshot
			targetNode.backingBuffer.putLong(pos + Node.SIZE_OF_SNAPSHOT_ID, Node.OPEN_VALIDITY); // until it is specifically invalidated
			targetNode.backingBuffer.put(pos + 2 * Node.SIZE_OF_SNAPSHOT_ID, auth); // with this authenticator
			targetNode.markDirty(false);
			if (copied)
				targetNode = node.tree.releaseNode(targetNode);
		} else {
			// make sure the last one is invalid starting from this snapshot id
			if (ca > 0)
				if (node.backingBuffer.getLong(pos + Node.SIZE_OF_SNAPSHOT_ID) == Node.OPEN_VALIDITY)
					node.backingBuffer.putLong(pos + Node.SIZE_OF_SNAPSHOT_ID, snapshotId - 1L);
			node.markDirty(false);
		}
	}

	@Override
	public byte[] getAuthenticator(Node node, long snapshotId)
			throws IOException {
		int count = node.getCountAuthenticator();
		int pos = node.getFirstAuthenticatorPos();
		for (int i = 0; i < count; i++) {
			long from = node.backingBuffer.getLong(pos);
			long to = node.backingBuffer
					.getLong(pos + Node.SIZE_OF_SNAPSHOT_ID);
			if (snapshotId >= from && (snapshotId <= to || to == Node.OPEN_VALIDITY)) {
				// bingo
				byte[] auth = new byte[Node.SIZE_OF_AUTHENTICATOR_DIGEST];
				node.backingBuffer
						.get(pos + 2 * Node.SIZE_OF_SNAPSHOT_ID, auth);
				return auth;
			}
		}
		// so, we didn't find one...
		return node.producePastAuthenticator(snapshotId);
	}

	@Override
	public void cleanupAfter(Node node, long snapshotId) {
		if (node.getCountAuthenticator() > 0 ) {
			int pos = node.getFirstAuthenticatorPos();
			long sidFrom = node.backingBuffer.getLong(pos);
			long sidTo = node.backingBuffer.getLong(pos + Node.SIZE_OF_SNAPSHOT_ID);
			if (sidFrom == snapshotId) {
				node.backingBuffer.delete(pos, ENTRY_SIZE);
				node.setCountAuthenticator(node.getCountAuthenticator() - 1);
			} else
				if (sidTo == Node.OPEN_VALIDITY || sidTo == snapshotId )
					node.backingBuffer.putLong(pos + Node.SIZE_OF_SNAPSHOT_ID, snapshotId - 1L);
		}
	}
	
	@Override
	public String toString(Node node, long snapshotId) {
		StringBuilder sb = new StringBuilder();
		int count = node.getCountAuthenticator();
		int pos = node.getFirstAuthenticatorPos();
		for (int i = 0; i < count; i++) {
			long sidFrom = node.backingBuffer.getLong(pos);
			pos += Node.SIZE_OF_SNAPSHOT_ID;
			long sidTo = node.backingBuffer.getLong(pos);
			pos += Node.SIZE_OF_SNAPSHOT_ID;
			byte[] auth = new byte[Node.SIZE_OF_AUTHENTICATOR_DIGEST];
			node.backingBuffer.get(pos, auth);
			pos += Node.SIZE_OF_AUTHENTICATOR_DIGEST;
			if( snapshotId == -1L || sidFrom <= snapshotId )
				sb.append(String.format("A[%d]=S<%d-%d>:%s%n", i, sidFrom, sidTo, Bytes.toString(auth)));
		}
		return sb.toString();
	}
}
