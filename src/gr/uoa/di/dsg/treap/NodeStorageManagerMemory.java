package gr.uoa.di.dsg.treap;

import gr.uoa.di.dsg.util.MemoryBuffer;

import java.io.IOException;
import java.util.ArrayList;

public class NodeStorageManagerMemory extends NodeStorageManager {
	
	ArrayList<byte[]> rawBuffers = new ArrayList<>();
	ArrayList<Long> snapshotRootNode = new ArrayList<>();
	ArrayList<Long> snpashotTimestamp = new ArrayList<>();
	private int maxNodeSize = 1024 * 1024;
	private int idCounter = 0;
	
	private long currentRoot = Tree.TERMINAL_NODE_ID;
	
	public NodeStorageManagerMemory() {
	}
	
	public NodeStorageManagerMemory(int maxNodeSize) {
		this.maxNodeSize = maxNodeSize;
	}

	public long getCurrentSnapshotId() {
		return snapshotRootNode.size() + 1;
	}
	
	public long getRoot(long snapshotId) {
		return snapshotRootNode.get((int)snapshotId - 1);
	}

	public long getTimestampOfSnapshot(long snapshotId) {
		return snpashotTimestamp.get((int)snapshotId - 1);
	}

	/*
	 * Retrieves the root node id of the current (open) snapshot
	 */
	public long getRoot() {
		return currentRoot;
	}

	public byte[] closeSnapshot(long rootNode, byte[] rootAuthenticator) {
		snapshotRootNode.add(rootNode);
		snpashotTimestamp.add(System.currentTimeMillis());
		return null;
	}

	public long nextId() {
		//return rawBuffers.size();
		return idCounter++;
	}

	protected void update(long id, MemoryBuffer buffer, boolean isNew) {
		if( isNew )
			while (rawBuffers.size() <= id)
				rawBuffers.add(null);
		byte[] raw = new byte[buffer.size()];
		buffer.write(raw);
		rawBuffers.set((int) id, raw);
	}

	protected byte[] obtain(long lid) {
		return rawBuffers.get((int) lid);
	}

	public void setRoot(long newRoot) {
		currentRoot = newRoot;
	}
	
	@Override
	public void close() throws IOException {
	}
	
	@Override
	public int getLargestNodeSize() {
		return 0;
	}

	@Override
	public int getMaxNodeSize() {
		return maxNodeSize;
	}
	
	@Override
	public long getFileSize() throws IOException {
		return 0;
	}
}
