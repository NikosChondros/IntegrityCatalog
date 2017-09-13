package gr.uoa.di.dsg.treap;

import java.io.IOException;

/**
 * This class abstracts away from Node all authenticator caching
 * All descendants should always be state-less as a single instance is passed around to all Node instances
 *
 */
public abstract class AuthenticatorCacheManager {
	public abstract boolean always();
	public abstract int entrySize();
	public abstract byte[] getCurrentAuthenticator(Node node, TreePath path) throws IOException;
	public abstract void cacheAuthenticator(Node node, long snapshotId, byte[] auth, TreePath path) throws IOException;
	public abstract byte[] getAuthenticator(Node node, long snapshotId) throws IOException;
	public abstract void cleanupAfter(Node node, long snapshotId);
	public abstract String toString(Node node, long snapshotId); 
}
