package gr.uoa.di.dsg.treap;

import java.io.IOException;

import edu.stanford.identiscape.mappedMemory.MappedMemory;
import edu.stanford.identiscape.skiplists.disk.SkipList;
import edu.stanford.identiscape.util.Bytes;
import gr.uoa.di.dsg.util.MemoryBuffer;
import gr.uoa.di.dsg.vsrm.RecordManager;

/*
 * Stores the root record of the current snapshot as record 0
 * Stores nodes at records in the file
 * Stores root snapshots and timestamps as entries in an AASL
 */
public class NodeStorageManagerVSRM extends NodeStorageManager {
	private static final long HEADER_RECORD_NUMBER = 0L;

	private class Header {
		public static final int SIZE_OF_LONG = Long.SIZE / Byte.SIZE;
		public static final int SIZE_OF_SNAPSHOT_ID = SIZE_OF_LONG;
		public static final int SIZE_OF_NODE_ID = SIZE_OF_LONG;
		
		public static final int CURRENT_ROOT_START = 0;
		public static final int CURRENT_ROOT_SIZE = SIZE_OF_NODE_ID;
		public static final int CURRENT_SNAPSHOT_ID_START = CURRENT_ROOT_START + CURRENT_ROOT_SIZE;
		public static final int CURRENT_SNAPSHOT_ID_SIZE = SIZE_OF_SNAPSHOT_ID;
		public static final int RECORD_SIZE = CURRENT_SNAPSHOT_ID_START + CURRENT_SNAPSHOT_ID_SIZE;
		
		byte[] raw;

		void readFrom(byte[] raw) {
			this.raw = raw;
		}
		
		void initialize() {
			this.raw = new byte[RECORD_SIZE];
			setCurrentRoot(Tree.TERMINAL_NODE_ID);
			setCurrentSnapshotId(1L);
		}
		
		long getCurrentSnapshotId() {
			return Bytes.toLong(raw, CURRENT_SNAPSHOT_ID_START);
		}
		
		void setCurrentSnapshotId(long v) {
			Bytes.longToBytes(v, raw, CURRENT_SNAPSHOT_ID_START);
		}
		
		long getCurrentRoot() {
			return Bytes.toLong(raw, CURRENT_ROOT_START);
		}
		
		void setCurrentRoot(long v) {
			Bytes.longToBytes(v, raw, CURRENT_ROOT_START);;
		}
		
	}
	
	Header header;
	private RecordManager rm;
	private long lastRecord;
	private SkipList skipList;
	private int sensitiveSize = Node.SIZE_OF_AUTHENTICATOR_DIGEST + Node.SIZE_OF_LONG;
	private int insensitiveSize = Node.SIZE_OF_NODE_ID;
	
	public NodeStorageManagerVSRM(RecordManager rm, MappedMemory mm) throws IOException {
		super();
		this.rm = rm;
		lastRecord = rm.getLastRecNo();
		header = new Header();
		if( lastRecord != -1L ) {
			header.readFrom( rm.get(HEADER_RECORD_NUMBER) );
			this.skipList = new SkipList(header.getCurrentSnapshotId(), mm); 
		} else {
			header.initialize();
			rm.add(HEADER_RECORD_NUMBER, header.raw);
			lastRecord = HEADER_RECORD_NUMBER;
			this.skipList = new SkipList(SkipList.NILLABEL, sensitiveSize, insensitiveSize, mm); 
		}
	}
	
	private void writeHeader() throws IOException {
		rm.update(HEADER_RECORD_NUMBER, header.raw);
	}

	public long getCurrentSnapshotId() {
		return header.getCurrentSnapshotId();
	}
	
	public long getRoot(long snapshotId) {
		byte[] insensitive = new byte[insensitiveSize];
		skipList.insensitive(snapshotId, insensitive, 0);
		return Bytes.toLong(insensitive, 0);
	}

	public long getTimestampOfSnapshot(long snapshotId) {
		byte[] sensitive = new byte[sensitiveSize];
		skipList.sensitive(snapshotId, sensitive, 0);
		return Bytes.toLong(sensitive, 0);
	}

	/*
	 * Retrieves the root node id of the current (open) snapshot
	 */
	public long getRoot() {
		return header.getCurrentRoot();
	}

	@Override
	public void setRoot(long newRoot) throws IOException {
		header.setCurrentRoot(newRoot);
		writeHeader();
	}
	
	public byte[] closeSnapshot(long rootNode, byte[] rootAuthenticator) throws IOException {
		byte[] sensitive = new byte[sensitiveSize];
		byte[] insensitive = new byte[insensitiveSize];
		Bytes.longToBytes(System.currentTimeMillis(), sensitive, 0);
		System.arraycopy(rootAuthenticator, 0, sensitive, Node.SIZE_OF_LONG, rootAuthenticator.length);
		Bytes.longToBytes(rootNode, insensitive, 0);
		byte[] stateAuth = skipList.append(sensitive, 0, insensitive, 0);
		skipList.commit();
		header.setCurrentSnapshotId(header.getCurrentSnapshotId() + 1L);
		writeHeader();
		return stateAuth;
	}

	public long nextId() {
		lastRecord++;
		return lastRecord;
	}

	@Override
	protected void update(long id, MemoryBuffer buffer, boolean isNew) throws IOException {
		if( isNew )
			rm.add(id, buffer);
		else
			rm.update(id, buffer);
	}

	@Override
	protected byte[] obtain(long lid) throws IOException {
		return rm.get(lid);
	}
	
	@Override
	public void close() throws IOException {
		rm.close();
		skipList.close();
	}
	@Override
	public int getLargestNodeSize() {
		return rm.getLargestRecordSize();
	}

	@Override
	public int getMaxNodeSize() {
		return rm.maxRecordLength();
	}
	
	@Override
	public long getFileSize() throws IOException {
		return rm.getFileSize();
	}
}
