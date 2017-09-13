package gr.uoa.di.dsg.treap;

import java.io.IOException;

import edu.stanford.identiscape.util.Bytes;

public class AuthenticatorCacheManagerAlways extends AuthenticatorCacheManager {
	private static final int ENTRY_SIZE = Node.SIZE_OF_SNAPSHOT_ID + Node.SIZE_OF_AUTHENTICATOR_DIGEST; 
	@Override
	public boolean always() {
		return true;
	}
	
	@Override
	public int entrySize() {
		return ENTRY_SIZE;
	}
	
	@Override
	public byte[] getCurrentAuthenticator(Node node, TreePath path) {
		byte[] ret = new byte[Node.SIZE_OF_AUTHENTICATOR_DIGEST];
		node.backingBuffer.get(node.getFirstAuthenticatorPos() + Node.SIZE_OF_SNAPSHOT_ID, ret);
		return ret;
	}
	
	@Override
	public void cacheAuthenticator(Node node, long snapshotId, byte[] auth, TreePath path) throws IOException {
		Node targetNode;
		boolean copied = false;
		if (node.canExpandBy(ENTRY_SIZE))
			targetNode = node;
		else {
			long newNodeId = node.copyNode(node.getLeft(), node.getRight(), node.getVersionedValue(), path); //this one cleanses 'node' and removes it from path
			targetNode = node.tree.fetchNode(newNodeId);
			copied = true;
		}
		//we need to insert a new entry
		int pos = targetNode.getFirstAuthenticatorPos();
		byte[] extra = new byte[Node.SIZE_OF_SNAPSHOT_ID + Node.SIZE_OF_AUTHENTICATOR_DIGEST];
		targetNode.backingBuffer.insert(pos, extra);
		targetNode.setCountAuthenticator(targetNode.getCountAuthenticator() + 1);
		targetNode.backingBuffer.putLong(pos, snapshotId);
		targetNode.backingBuffer.put(pos + Node.SIZE_OF_SNAPSHOT_ID, auth);
		targetNode.markDirty(false);
		if (copied)
			targetNode = node.tree.releaseNode(targetNode);
	}
	
	@Override
	public byte[] getAuthenticator(Node node, long snapshotId) {
		int count = node.getCountAuthenticator();
		int pos = node.getFirstAuthenticatorPos();
		for(int i = 0; i < count; i++)
			if( node.backingBuffer.getLong(pos) <= snapshotId ) {
				byte[] auth = new byte[Node.SIZE_OF_AUTHENTICATOR_DIGEST]; 
				node.backingBuffer.get(pos + Node.SIZE_OF_SNAPSHOT_ID, auth);
				return auth;
			} else
				pos += Node.SIZE_OF_SNAPSHOT_ID + Node.SIZE_OF_AUTHENTICATOR_DIGEST; // move to the next 
		throw new RuntimeException("Node.getAuthenticator(snapshotId) could not locate any authenticator in current Node");
	}

	@Override
	public void cleanupAfter(Node node, long snapshotId) {
		int pos = node.getFirstAuthenticatorPos();
		if( node.backingBuffer.getLong(pos) == snapshotId ) {
			node.backingBuffer.delete(pos, ENTRY_SIZE);
			node.setCountAuthenticator(node.getCountAuthenticator() - 1);
		}
	}
	
	@Override
	public String toString(Node node, long snapshotId) {
		StringBuilder sb = new StringBuilder();
		int count = node.getCountAuthenticator();
		int pos = node.getFirstAuthenticatorPos();
		for(int i=0; i<count;i++) {
			long sid = node.backingBuffer.getLong(pos);
			byte[] auth = new byte[Node.SIZE_OF_AUTHENTICATOR_DIGEST]; 
			node.backingBuffer.get(pos + Node.SIZE_OF_SNAPSHOT_ID, auth);
			if (snapshotId == -1L || sid <= snapshotId )
				sb.append(String.format("A[%d]=S%d:%s%n", i, sid, Bytes.toString(auth)));
			pos += Node.SIZE_OF_SNAPSHOT_ID + Node.SIZE_OF_AUTHENTICATOR_DIGEST; // move to the next
		}
		return sb.toString();
	}
}
