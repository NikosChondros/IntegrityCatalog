package gr.uoa.di.dsg.treap;

import java.io.Closeable;
import java.io.IOException;
import java.util.TreeMap;

import edu.stanford.identiscape.util.ByteArrayRegion;
import gr.uoa.di.dsg.util.MemoryBuffer;

public abstract class NodeStorageManager implements Closeable {
	private class CacheElement {
		int refCount;
		Node node;
		boolean dirty;
		boolean isNew;
		public CacheElement(Node node, boolean isNew) {
			this.node = node;
			this.refCount = 1;
			this.isNew = isNew;
			this.dirty = false;
		}
	}
	/*
	 * loaded currently does not cache anything outside the scope of the current operation (beginWork/commitWork)
	 * A good enhancement would be to actually cache hot nodes up to a specific bound.
	 * However, due to the very variable size of each node, this needs a great deal of attention so that we don't hog JVM heap with our cache
	 */
	private TreeMap<Long, CacheElement> loaded = new TreeMap<>();

	public void markDirty(Node node) throws IOException {
		loaded.get(node.getId()).dirty = true;
	}

	public Node create(Tree tree, ByteArrayRegion key, VersionedValue vvalue, long left, long right) throws IOException {
		byte[] raw = new byte[Node.estimateNewNodeSize(key, vvalue.value)];
		long id = nextId();
		CacheElement e = new CacheElement(null, true);
		loaded.put(id, e); //create an entry for it, so markDirty() in constructor works 
		Node node = new Node(tree, tree.getACM(), id, new MemoryBuffer(raw), key, vvalue, left, right);
		e.node =  node; //and update the cache element with the proper object
		return node;
	}
	
	public Node fetch(long lid, Tree tree, AuthenticatorCacheManager acm) throws IOException {
		CacheElement e = loaded.get(lid);
		if( e == null ) {
			e = new CacheElement(new Node(tree, acm, lid, new MemoryBuffer(obtain(lid))), false);
			loaded.put(lid, e);
		} else {
			e.refCount += 1;
		}
		return e.node;
	}
	
	public void release(Node node) {
		CacheElement e = loaded.get(node.getId());
		e.refCount -= 1;
		if( e.refCount <= 0 )
			if( ! e.dirty )
				loaded.remove(node.getId());
	}

	protected abstract void update(long id, MemoryBuffer buffer, boolean isNew) throws IOException;
	protected abstract byte[] obtain(long lid) throws IOException;

	/**
	 * Retrieves the root node id of the current (open) snapshot
	 */
	public abstract long getRoot();
	public abstract long getRoot(long snapshotId);
	public abstract long getCurrentSnapshotId();
	public abstract long getTimestampOfSnapshot(long snapshotId);
	public abstract byte[] closeSnapshot(long rootNode, byte[] rootAuthenticator) throws IOException;
	public abstract long nextId();
	public abstract void setRoot(long newRoot) throws IOException;
	public abstract void close() throws IOException;
	public abstract int getLargestNodeSize();
	public abstract int getMaxNodeSize();
	public abstract long getFileSize() throws IOException;
	
	public void beginWork() {
	}
	
	public void endWork() throws IOException {
		for( long id : loaded.keySet() ) {
			CacheElement e = loaded.get(id);
			if( e.dirty )
				update(id, e.node.backingBuffer, e.isNew);
		}
		loaded.clear();
	}
}
