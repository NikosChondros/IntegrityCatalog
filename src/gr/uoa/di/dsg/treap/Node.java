package gr.uoa.di.dsg.treap;

import edu.stanford.identiscape.util.ByteArrayRegion;
import gr.uoa.di.dsg.util.MemoryBuffer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class Node {
	private static final String PRIORITY_DIGEST_ALGORITHM = "SHA-1";
	private static final String AUTHENTICATOR_DIGEST_ALGORITHM = "SHA-1";
	public static final byte[] AUTHENTICATOR_OF_TERMINAL_NODE;
	/*
	 * Simple fat node is logically structured like this:
	 * 		byte[] key
	 * 		byte[] priority
	 * 		byte modifiedInCurrentSnapshot
	 * 		Map<SnapshotId -> NodeId> left
	 * 		Map<SnapshotId -> NodeId> right
	 * 		Map<SnapshotId -> <length, data> value
	 * 		Map<SnapshotId -> Authenticator> authenticator
	 * And physically like this (all arrays are reverse sorted, i.e. the first element is the most recent one)
	 * 		UShort keyLength, countLeft, countRight, countValue, countAuthenticator
	 * 		20 bytes priority
	 * 		byte modified
	 * 		byte[] key
	 * 		[ long snapshotId, long nodeId] left
	 * 		[ long snapshotId, long nodeId] right
	 * 		[ long snapshotId, x bytes authenticator] authenticators
	 * 		[ long snapshotId, short valueLength, byte[] valueData ]
	 */
	
	public static final long OPEN_VALIDITY = 0L;
	private static final MessageDigest priorityDigest;
	public static final int SIZE_OF_PRIORITY_DIGEST;
	protected static final MessageDigest authenticatorDigest;
	public static final int SIZE_OF_AUTHENTICATOR_DIGEST;
	protected static final MessageDigest valueDigest;
	static {
		try {
			priorityDigest = MessageDigest.getInstance(PRIORITY_DIGEST_ALGORITHM);
			SIZE_OF_PRIORITY_DIGEST = priorityDigest.getDigestLength();
			
			authenticatorDigest = MessageDigest.getInstance(AUTHENTICATOR_DIGEST_ALGORITHM);
			SIZE_OF_AUTHENTICATOR_DIGEST = authenticatorDigest.getDigestLength();
			valueDigest = MessageDigest.getInstance(AUTHENTICATOR_DIGEST_ALGORITHM);
			
			AUTHENTICATOR_OF_TERMINAL_NODE = new byte[SIZE_OF_AUTHENTICATOR_DIGEST];
			Arrays.fill(AUTHENTICATOR_OF_TERMINAL_NODE, (byte) 0);
			
		} catch (NoSuchAlgorithmException ex) {
			throw new RuntimeException("Error in static initializer for Node MessageDigest", ex);
		}
	}
	
	public static final int SIZE_OF_BYTE = Byte.SIZE / Byte.SIZE; //Harmony in all its glory :-)
	public static final int SIZE_OF_SHORT = Short.SIZE / Byte.SIZE;
	public static final int SIZE_OF_LONG = Long.SIZE / Byte.SIZE;
	
	public static final int SIZE_OF_SNAPSHOT_ID = SIZE_OF_LONG;
	public static final int SIZE_OF_NODE_ID = SIZE_OF_LONG;
	
	public static final int KEY_LENGTH_START = 0;
	public static final int KEY_LENGTH_SIZE = SIZE_OF_SHORT;
	public static final int COUNT_LEFT_START = KEY_LENGTH_START + KEY_LENGTH_SIZE;
	public static final int COUNT_LEFT_SIZE = SIZE_OF_SHORT;
	public static final int COUNT_RIGHT_START = COUNT_LEFT_START + COUNT_LEFT_SIZE;
	public static final int COUNT_RIGHT_SIZE = SIZE_OF_SHORT;
	public static final int COUNT_AUTHENTICATOR_START = COUNT_RIGHT_START + COUNT_RIGHT_SIZE;
	public static final int COUNT_AUTHENTICATOR_SIZE = SIZE_OF_SHORT;
	public static final int COUNT_VALUE_START = COUNT_AUTHENTICATOR_START + COUNT_AUTHENTICATOR_SIZE;
	public static final int COUNT_VALUE_SIZE = SIZE_OF_SHORT;
	public static final int PRIORITY_START = COUNT_VALUE_START + COUNT_VALUE_SIZE;
	public static final int PRIORITY_SIZE = SIZE_OF_PRIORITY_DIGEST;
	public static final int MODIFIED_START = PRIORITY_START + PRIORITY_SIZE;
	public static final int MODIFIED_SIZE = SIZE_OF_BYTE;
	
	public static final int FIXED_SIZE = MODIFIED_START + MODIFIED_SIZE;
	public static final int KEY_START = FIXED_SIZE;
	
	private long myId;
	protected Tree tree;
	private AuthenticatorCacheManager acm;
	protected MemoryBuffer backingBuffer;
	
	public static int estimateNewNodeSize(ByteArrayRegion key, ByteArrayRegion value) {
		return key.length + value.length + FIXED_SIZE + 
				SIZE_OF_SNAPSHOT_ID + SIZE_OF_NODE_ID + //snapshot and left pointer
				SIZE_OF_SNAPSHOT_ID + SIZE_OF_NODE_ID + //snapshot and right pointer
				SIZE_OF_SNAPSHOT_ID + SIZE_OF_NODE_ID + SIZE_OF_SHORT; //snapshot and value length
	}

	protected Node(Tree tree, AuthenticatorCacheManager acm, long id, MemoryBuffer buffer, ByteArrayRegion key, VersionedValue vvalue, long left, long right) throws IOException {
		this.tree = tree;
		this.acm = acm;
		this.myId = id;
		this.backingBuffer = buffer;
		
		long snapshotId = tree.getCurrentSnapshotId();
		
		backingBuffer.putUShort(KEY_LENGTH_START, key.length);
		backingBuffer.put(KEY_START, key);
		
		priorityDigest.reset();
		priorityDigest.update(key.buffer, key.start, key.length);
		backingBuffer.put(PRIORITY_START, priorityDigest.digest());
		
		setCountLeft(1);
		setCountRight(1);
		setCountValue(1);
		setCountAuthenticator(0);

		int pos = getFirstLeftPos();
		backingBuffer.putLong(pos, snapshotId);
		backingBuffer.putLong(pos + SIZE_OF_SNAPSHOT_ID, left);
		
		pos = getFirstRightPos();
		backingBuffer.putLong(pos, snapshotId);
		backingBuffer.putLong(pos + SIZE_OF_SNAPSHOT_ID, right);

		pos = getFirstValuePos();
		backingBuffer.putLong(pos, vvalue.snapshotId);
		backingBuffer.putUShort(pos + SIZE_OF_SNAPSHOT_ID, vvalue.value.length);
		backingBuffer.put(pos + SIZE_OF_SNAPSHOT_ID + SIZE_OF_SHORT, vvalue.value);
		
		markDirty();
	}
	
	protected Node(Tree tree, AuthenticatorCacheManager acm, long id, MemoryBuffer buffer) {
		this.tree = tree;
		this.acm = acm;
		this.myId = id;
		this.backingBuffer = buffer;
	}

	protected MemoryBuffer getBuffer() {
		return backingBuffer;
	}
	
	public long getId() {
		return myId;
	}
	
	public long getOriginalSnapshot() {
		//retrieve the snapshotId of the first "left" pointer.
		//This should be faster (O(1)) than going through the list of values
		int pos = getFirstLeftPos() + (getCountLeft() - 1) * (SIZE_OF_SNAPSHOT_ID + SIZE_OF_NODE_ID);
		return backingBuffer.getLong(pos);
	}
	
	private final int getFirstLeftPos() {
		return KEY_START + getKeyLength();
	}
	
	private final int getFirstRightPos() {
		return getFirstLeftPos() + getCountLeft() * (SIZE_OF_SNAPSHOT_ID + SIZE_OF_NODE_ID);
	}
	
	protected final int getFirstAuthenticatorPos() {
		return getFirstRightPos() + getCountRight() * (SIZE_OF_SNAPSHOT_ID + SIZE_OF_NODE_ID);
	}
	
	private final int getFirstValuePos() {
		return getFirstAuthenticatorPos() + getCountAuthenticator() * acm.entrySize();
	}
	
	private final int getKeyLength() {
		return backingBuffer.getUShort(KEY_LENGTH_START);
	}
	
	private final int getCountLeft() {
		return backingBuffer.getUShort(COUNT_LEFT_START);
	}
	
	private void setCountLeft(int v) {
		backingBuffer.putUShort(COUNT_LEFT_START, v);
	}
	
	private final int getCountRight() {
		return backingBuffer.getUShort(COUNT_RIGHT_START);
	}
	
	private void setCountRight(int v) {
		backingBuffer.putUShort(COUNT_RIGHT_START, v);
	}
	
	protected final int getCountAuthenticator() {
		return backingBuffer.getUShort(COUNT_AUTHENTICATOR_START);
	}
	
	protected void setCountAuthenticator(int v) {
		backingBuffer.putUShort(COUNT_AUTHENTICATOR_START, v);
	}
	
	private final int getCountValue() {
		return backingBuffer.getUShort(COUNT_VALUE_START);
	}
	
	private void setCountValue(int v) {
		backingBuffer.putUShort(COUNT_VALUE_START, v);
	}
	
	public long getLeft() {
		//*TRACE*/System.err.format("%d GL %d%n", myId, backingBuffer.getLong(getFirstLeftPos() + SIZE_OF_SNAPSHOT_ID));
		return backingBuffer.getLong(getFirstLeftPos() + SIZE_OF_SNAPSHOT_ID);
	}
	
	/**
	 * Historic search for the left child of this node as it was defined for the specified snapshot 
	 * @param snapshotId
	 * @return
	 */
	public long getLeft(long snapshotId) {
		int count = getCountLeft();
		int pos = getFirstLeftPos();
		for(int i = 0; i < count; i++)
			if( backingBuffer.getLong(pos) <= snapshotId )
				return  backingBuffer.getLong(pos + SIZE_OF_SNAPSHOT_ID);
			else
				pos += SIZE_OF_SNAPSHOT_ID + SIZE_OF_NODE_ID; // move to the next
		throw new IllegalArgumentException(String.format("Could not locate left pointer for at %d for snapshot %d", myId, snapshotId));
	}
	
	protected final boolean canExpandBy(int size) {
		//always allow one extra left or right pointer update which might result by a rotate due to the insertion of the new copy of the node
		int curr = backingBuffer.size();
		boolean ret = backingBuffer.size() + size <= tree.getMaxNodeSize();
		//*TRACE*/System.out.format("%d.canExpandBy(%d)->%b%n", myId, size, ret);
		return ret;
	}
	
	private void dumpStack(String s) {
		StackTraceElement[] stack = Thread.currentThread().getStackTrace();
		System.out.println(s + " Stack trace:");
		for(int i=2; i<50; i++) {
			StackTraceElement se = stack[i];
			String out = se.toString();
			if (out.startsWith("gr.uoa"))
				System.out.println("    " + se.toString());
			else
				break;
		}
	}

	protected long copyNode(long left, long right, VersionedValue vvalue, TreePath path) throws IOException {
		
		//*TRACE*/dumpStack("copyNode");
		//the following code actually "undoes" all changes that happened in this node, at the current epoch.
		//These changes are transferred to the new copy, and the old one remains as it was until the last snapshotId
		//*TRACE*/String nodePre = this.toString().replace('\n', '\t');
		long snapshotId = tree.getCurrentSnapshotId();
		int pos = getFirstLeftPos();
		if (backingBuffer.getLong(pos) == snapshotId ) {
			backingBuffer.delete(pos, SIZE_OF_SNAPSHOT_ID + SIZE_OF_NODE_ID);
			setCountLeft(getCountLeft() - 1);
		}
		pos = getFirstRightPos();
		if (backingBuffer.getLong(pos) == snapshotId ) {
			backingBuffer.delete(pos, SIZE_OF_SNAPSHOT_ID + SIZE_OF_NODE_ID);
			setCountRight(getCountRight() - 1);
		}
		pos = getFirstValuePos();
		if (backingBuffer.getLong(pos) == snapshotId ) {
			backingBuffer.delete(pos, SIZE_OF_SNAPSHOT_ID + SIZE_OF_SHORT + backingBuffer.getUShort(pos + SIZE_OF_SNAPSHOT_ID));
			setCountValue(getCountValue() - 1);
		}
		acm.cleanupAfter(this, snapshotId);
		
		Node newNode = tree.createNode(getKeyRegion(), vvalue, left, right);
		long ret = newNode.getId();
		tree.incrNodeCopies();

		markDirty(false); //it is dirty for storage manager but not for authenticator recalculation, as everything in this snapshot has been cleared
		//*TRACE*/String nodePost = this.toString().replace('\n', '\t');
		//*TRACE*/String newNodeStr = newNode.toString().replace('\n', '\t');

		//*TRACE*/String pathPre = path.toString();
		
		//*TRACE*/System.out.format("%d copiedTo %d I pre=%s ...%n", myId, newNode.myId, pathPre);

		//following updates in path may trigger recursive calls to copyNode
		
		//and now update the "path" with the new copy of the node
		//there is a single edge in the path that points to our old nodeId
		//replace it's pointer with id of newNode
		path.replaceEdgeTarget(myId, newNode.getId()); //this links to the new node in the linked list of the path
		//*TRACE*/String pathPost1 = path.toString();
		
		//there is a single edge in the path that begins from the old node and points somewhere
		//replace this edge with one that starts in the new node and points to the same child node 
		path.replaceEdgeSource(this, newNode); //and this replaces the current node with the new one, in the linked list of the path
		
		//*TRACE*/String pathPost2 = path.toString();
		//*TRACE*/System.out.format("%d.copiedTo %d F pre=%s post1=%s post2=%s%n", myId, newNode.myId, pathPre, pathPost1, pathPost2);
		
		//*TRACE*/String sKey; try {sKey = new String(getKey(), "UTF-8"); } catch (UnsupportedEncodingException ex) {sKey = "ERROR";}
		//*TRACE*/System.out.format("%d.copiedTo %d @S=%d key=%s%n", myId, newNode.myId, snapshotId, sKey);
		//*TRACE*/System.out.format("  old.pre=%s%n", nodePre);
		//*TRACE*/System.out.format("  old.post=%s%n", nodePost);
		//*TRACE*/System.out.format("  new.post=%s%n", newNodeStr);
		tree.releaseNode(newNode);
		return ret;
	}
	
	public void setLeft(long nodeId, TreePath path) throws IOException {
		long snapshotId = tree.getCurrentSnapshotId();
		int pos = getFirstLeftPos();
		if( snapshotId != backingBuffer.getLong(pos) ) {
			//we need to insert a new entry
			if (canExpandBy(SIZE_OF_SNAPSHOT_ID + SIZE_OF_NODE_ID)) {
				byte[] extra = new byte[SIZE_OF_SNAPSHOT_ID + SIZE_OF_NODE_ID];
				backingBuffer.insert(pos, extra);
				setCountLeft(getCountLeft() + 1);
				backingBuffer.putLong(pos, snapshotId);
				backingBuffer.putLong(pos + SIZE_OF_SNAPSHOT_ID, nodeId);
				markDirty();
			} else {
				copyNode(nodeId, getRight(), getVersionedValue(), path);
			}
		} else {
			backingBuffer.putLong(pos + SIZE_OF_SNAPSHOT_ID, nodeId);
			markDirty();
		}
	}
	
	public long getRight() {
		//*TRACE*/System.err.format("%d GR %d%n", myId, backingBuffer.getLong(getFirstRightPos() + SIZE_OF_SNAPSHOT_ID));
		return backingBuffer.getLong(getFirstRightPos() + SIZE_OF_SNAPSHOT_ID);
	}
	
	/**
	 * Historic search for the right child of this node as it was defined for the specified snapshot 
	 * @param snapshotId
	 * @return
	 */
	public long getRight(long snapshotId) {
		int count = getCountRight();
		int pos = getFirstRightPos();
		for(int i = 0; i < count; i++)
			if( backingBuffer.getLong(pos) <= snapshotId )
				return  backingBuffer.getLong(pos + SIZE_OF_SNAPSHOT_ID);
			else
				pos += SIZE_OF_SNAPSHOT_ID + SIZE_OF_NODE_ID; // move to the next
		throw new IllegalArgumentException(String.format("Could not locate right pointer for at %d for snapshot %d", myId, snapshotId));
	}
	
	public void setRight(long nodeId, TreePath path) throws IOException {
		long snapshotId = tree.getCurrentSnapshotId();
		int pos = getFirstRightPos();
		if( snapshotId != backingBuffer.getLong(pos) ) {
			//we need to insert a new entry
			if (canExpandBy(SIZE_OF_SNAPSHOT_ID + SIZE_OF_NODE_ID)) {
				byte[] extra = new byte[SIZE_OF_SNAPSHOT_ID + SIZE_OF_NODE_ID];
				backingBuffer.insert(pos, extra);
				setCountRight(getCountRight() + 1);
				backingBuffer.putLong(pos, snapshotId);
				backingBuffer.putLong(pos + SIZE_OF_SNAPSHOT_ID, nodeId);
				markDirty();
			} else {
				copyNode(getLeft(), nodeId, getVersionedValue(), path);
			}
		} else {
			backingBuffer.putLong(pos + SIZE_OF_SNAPSHOT_ID, nodeId);
			markDirty();
		}
	}
	
	private ByteArrayRegion getKeyRegion() {
		return backingBuffer.extract(KEY_START, getKeyLength());
	}

	public byte[] getKey() {
		byte[] ret = new byte[getKeyLength()]; 
		backingBuffer.get(KEY_START, ret);
		return ret;
	}
	
	public byte[] getValue() {
		int pos = getFirstValuePos();
		byte[] ret = new byte[backingBuffer.getUShort(pos + SIZE_OF_SNAPSHOT_ID)]; 
		backingBuffer.get(pos + SIZE_OF_SNAPSHOT_ID + SIZE_OF_SHORT, ret);
		return ret;
	}
	
	public VersionedValue getVersionedValue() {
		int pos = getFirstValuePos();
		byte[] value = new byte[backingBuffer.getUShort(pos + SIZE_OF_SNAPSHOT_ID)]; 
		backingBuffer.get(pos + SIZE_OF_SNAPSHOT_ID + SIZE_OF_SHORT, value);
		return new VersionedValue(backingBuffer.getLong(pos), value);
	}
	
	/**
	 * if the current node is modified in the specified snapshotId, return the appropriate change
	 */  
	public NodeChanges getNodeChanges(long snapshotId) {
		//is the latest value timestamped in the specified epoch (snapshotId)?
		int pos = getFirstValuePos();
		if (backingBuffer.getLong(pos) == snapshotId) {
			//if the original node snapshot is the same as the requested one, then this is an append;
			//retrieve the snapshotId of the first "left" pointer, which should be faster (O(1)) than going through the list of values
			pos = getFirstLeftPos() + (getCountLeft() - 1) * (SIZE_OF_SNAPSHOT_ID + SIZE_OF_NODE_ID);
			boolean isAppend = backingBuffer.getLong(pos) == snapshotId;
			return new NodeChanges(isAppend, getKey(), getValue());
		} else 
			return null;
	}
	
	/**
	 * Historic search for the value as it was defined for the specified snapshot 
	 * @param snapshotId
	 * @return
	 */
	public byte[] getValue(long snapshotId) {
		int count = getCountValue();
		int pos = getFirstValuePos();
		for(int i = 0; i < count; i++)
			if( backingBuffer.getLong(pos) <= snapshotId ) {
				pos += SIZE_OF_SNAPSHOT_ID;
				int length = backingBuffer.getUShort(pos);
				pos += SIZE_OF_SHORT;
				byte[] ret = new byte[length]; 
				backingBuffer.get(pos, ret);
				return ret;
			} else
				pos += SIZE_OF_SNAPSHOT_ID + SIZE_OF_SHORT + backingBuffer.getUShort(pos + SIZE_OF_SNAPSHOT_ID); // move to the next
		throw new IllegalArgumentException(String.format("Could not locate right pointer for at %d for snapshot %d", myId, snapshotId));
	}
	
	protected static NodePayload decodePayload(byte[] payload) {
		//that is, decode snapshotId, length and value
		MemoryBuffer temp = new MemoryBuffer(payload);
		long snapshotId = temp.getLong(0);
		byte[] value = new byte[temp.getUShort(SIZE_OF_SNAPSHOT_ID)];
		temp.get(SIZE_OF_SNAPSHOT_ID + SIZE_OF_SHORT, value);
		return new NodePayload(snapshotId, value);
	}
	
	/**
	 * Historic search for the "payload" as it was defined for the specified snapshot 
	 * The payload is the tuple <snapshotId, value_length, value_bytes>
	 * @param snapshotId
	 * @return
	 */
	private ByteArrayRegion getRawPayload(long snapshotId) {
		int count = getCountValue();
		int pos = getFirstValuePos();
		for(int i = 0; i < count; i++) {
			int length = backingBuffer.getUShort(pos + SIZE_OF_SNAPSHOT_ID) + SIZE_OF_SNAPSHOT_ID + SIZE_OF_SHORT;
			if( backingBuffer.getLong(pos) <= snapshotId )
				return backingBuffer.extract(pos, length);
			else
				pos += length;
		}
		throw new RuntimeException("Node.getRawPayload(snapshotId) could not locate any value in current Node");
	}
	
	/**
	 * Historic search for the "payload" as it was defined for the specified snapshot 
	 * The payload is the tuple <snapshotId, value_length, value_bytes>
	 * @param snapshotId
	 * @return
	 */
	public byte[] getPayload(long snapshotId) {
		ByteArrayRegion region = getRawPayload(snapshotId);
		byte[] ret = new byte[region.length]; 
		System.arraycopy(region.buffer, region.start,  ret, 0,  region.length);
		return ret;
	}
	
	public static String payloadToString(byte[] payload) {
		MemoryBuffer mb = new MemoryBuffer(payload);
		long s = mb.getLong(0);
		int l = mb.getUShort(SIZE_OF_SNAPSHOT_ID);
		byte[] bv = new byte[l];
		mb.get(SIZE_OF_SNAPSHOT_ID + SIZE_OF_SHORT, bv);
		String v;
		try {v = new String(bv, "UTF-8"); }
		catch (UnsupportedEncodingException ex) {v = "ERROR";}

		return String.format("(S=%d L=%d V=%s)", s, l, v);
	}
	
	public void setValue(byte[] value, TreePath path) throws IOException {
		long snapshotId = tree.getCurrentSnapshotId();
		int pos = getFirstValuePos();
		
		if( snapshotId == backingBuffer.getLong(pos) ) {
			//modify current value in place
			int currentLength = backingBuffer.getUShort(pos + SIZE_OF_SNAPSHOT_ID);
			int diff = currentLength - value.length;
			int currentStart = pos + SIZE_OF_SNAPSHOT_ID + SIZE_OF_SHORT;
			if ( diff == 0 ) {
				//no size difference, simply replace the value in place
				backingBuffer.put(currentStart, value);
				markDirty();
			} else {
				if( diff > 0 ) { // new value smaller that older one
					backingBuffer.put(currentStart, value);
					backingBuffer.delete(currentStart + value.length, diff);
					backingBuffer.putUShort(pos + SIZE_OF_SNAPSHOT_ID, value.length);
					markDirty();
				} else { //new value larger than older one
					if (canExpandBy(0 - diff)) {
						byte[] copy = new byte[value.length];
						System.arraycopy(value, 0, copy, 0, value.length);
						backingBuffer.replace(currentStart, currentLength, copy);
    					backingBuffer.putUShort(pos + SIZE_OF_SNAPSHOT_ID, value.length);
						markDirty();
					} else
						copyNode(getLeft(), getRight(), new VersionedValue(snapshotId, value), path);
				}
			}
		} else {
			if (canExpandBy(SIZE_OF_SNAPSHOT_ID + SIZE_OF_SHORT + value.length)) {
				//need to create a new one, so insert it at the head of the list
				byte[] extra = new byte[SIZE_OF_SNAPSHOT_ID + SIZE_OF_SHORT + value.length];
				backingBuffer.insert(pos, extra);
				setCountValue(getCountValue() + 1);
				backingBuffer.putLong(pos, snapshotId);
				backingBuffer.putUShort(pos + SIZE_OF_SNAPSHOT_ID, value.length);
				backingBuffer.put(pos + SIZE_OF_SNAPSHOT_ID + SIZE_OF_SHORT, value);
				markDirty();
			} else
				copyNode(getLeft(), getRight(), new VersionedValue(snapshotId, value), path);
		}
	}
	
	public byte[] getPriority() {
		byte[] ret = new byte[PRIORITY_SIZE]; 
		backingBuffer.get(PRIORITY_START, ret); 
		return ret;
	}
	
	protected boolean getModified() {
		return backingBuffer.getByte(MODIFIED_START) == (byte) 1;
	}
	
	private void setModified(boolean v) {
		backingBuffer.putByte(MODIFIED_START, (byte) (v ? 1 : 0));
	}
	
	protected void markDirty() throws IOException {
		markDirty(true);
	}
	
	protected void markDirty(boolean modified) throws IOException {
		setModified(modified);
		tree.markDirty(this);
	}
	
	/**
	 * This one is optimized to not cause a disk write if node is already marked as modified
	 * @param modified
	 * @throws IOException
	 */
	protected void markModified() throws IOException {
		if(! getModified() ) {
			setModified(true);
			tree.markDirty(this);
		}
	}
	
	public int compareKey(ByteArrayRegion other) {
		return backingBuffer.compare(KEY_START, getKeyLength(), other.buffer, other.start, other.length);
	}

	public int compareKey(byte[] other) {
		return backingBuffer.compare(KEY_START, getKeyLength(), other, 0, other.length);
	}

	public int comparePriority(Node other) {
		return backingBuffer.compare(PRIORITY_START, PRIORITY_SIZE, other.getPriority(), 0, PRIORITY_SIZE);
	}

	public byte[] getCurrentAuthenticator(TreePath path, Consumer<Node> changesRecorder) throws IOException {
		if( getModified() ) {
			byte[] mine = produceCurrentAuthenticator(path, changesRecorder);
			long idOfLast = path.last().follow();
			if ( idOfLast == myId ) {
				//I am still ... me. That is, I was not copied for whatever reason.
				acm.cacheAuthenticator(this, tree.getCurrentSnapshotId(), mine, path); // will also clear modified flag(if actually cached)
			} else {
				//Ooops, identity crisis. Looks like I was copied. Now, store the authenticator to my new self
				//*TRACE*/System.out.format("***@getCurrentAuth: I was copied!!! Was %d, new me=%d%n", myId, idOfLast);
				Node myNewSelf = tree.fetchNode(idOfLast);
				acm.cacheAuthenticator(myNewSelf, tree.getCurrentSnapshotId(), mine, path); // will also clear modified flag(if actually cached)
				myNewSelf = tree.releaseNode(myNewSelf);
			}
			return mine;
		} else {
			return acm.getCurrentAuthenticator(this, path);
		}
	}
	
	protected byte[] produceCurrentAuthenticator(TreePath path, Consumer<Node> changesRecorder) throws IOException {
//		System.out.format("at %d back from left.produceAuthenticator() pre=(left=%d, right=%d) post=(last=%d left=%d right=%d)%n", 
//		myId, nleft, nright, path.last().follow(), getLeft(), getRight());
		//*TRACE*/long leftPtr = getLeft();
		//*TRACE*/long rightPtr = getRight();

		byte[] leftPart, rightPart;
		ByteArrayRegion payload;
		
		Node node = this;

		path.add(node.getLeftEdge());
		leftPart = tree.produceAuthenticator(path, changesRecorder);
		path.removeLast();
		
		long idOfLast = path.last().follow();
		if ( idOfLast != myId ) {
			//Ooops, identity crisis. Looks like I was copied. Now, read right/value from my new self (I may have been cleansed)
			node = tree.fetchNode(idOfLast); 
			//*TRACE*/System.out.format("***@produceCurrentAuth: I was copied!!! Was %d (%d,%d), new me=%d(%d,%d)%n", 
			//*TRACE*/		myId, leftPtr, rightPtr, idOfLast, node.getLeft(), node.getRight());
		}
		
		path.add(node.getRightEdge());
		rightPart = tree.produceAuthenticator(path, changesRecorder);
		path.removeLast();
		
		//payload is: 
		//the snapshotId, the value length and the value bytes.
		//later on, all these things will be fed into the proof and can be recovered from there
		int pos = node.getFirstValuePos();
		payload = node.backingBuffer.extract(pos, node.backingBuffer.getUShort(pos + SIZE_OF_SNAPSHOT_ID) + SIZE_OF_SNAPSHOT_ID + SIZE_OF_SHORT);
		changesRecorder.accept(this);
		if ( idOfLast != myId )
			node = tree.releaseNode(node);
		
		ByteArrayRegion key = backingBuffer.extract(KEY_START, getKeyLength()); //this we can read from the old node, still the same
		return calculateAuthenticator(key, payload, leftPart, rightPart);
	}
	
	protected byte[] producePastAuthenticator(long snapshotId) throws IOException {
		byte[] left = tree.getAuthenticator(snapshotId, getLeft(snapshotId));
		byte[] right = tree.getAuthenticator(snapshotId, getRight(snapshotId));
		ByteArrayRegion key = backingBuffer.extract(KEY_START, getKeyLength());
		//payload is: 
		//the snapshotId, the value length and the value bytes.
		//later on, all these things will be fed into the proof and can be recovered from there
		
		ByteArrayRegion payload = getRawPayload(snapshotId);
		return calculateAuthenticator(key, payload, left, right);
	}
	
	public byte[] getAuthenticator(long snapshotId) throws IOException {
		return acm.getAuthenticator(this, snapshotId);
	}
	
	protected static byte[] calculateAuthenticator(ByteArrayRegion key, ByteArrayRegion payload, byte[] left, byte[] right) {
		authenticatorDigest.reset();
		authenticatorDigest.update(left);
		
		authenticatorDigest.update(key.buffer, key.start, key.length);
		//feed the payload to the value digest
		valueDigest.reset();
		valueDigest.update(payload.buffer, payload.start, payload.length);
		//and now feed the hash of the full value buffer to the authenticator digest algorithm
		authenticatorDigest.update(valueDigest.digest());
		authenticatorDigest.update(right);
		return authenticatorDigest.digest();
	}
	
	private class EdgeClass implements TreeEdge {
		private boolean isLeft;
		private Node host;
		
		public EdgeClass(Node host, boolean isLeft) {
			super();
			this.host = host;
			this.isLeft = isLeft;
		}

		@Override
		public long follow() {
			if( isLeft )
				return host.getLeft();
			else
				return host.getRight();
		}
		
		@Override
		public void replace(long other, TreePath path) throws IOException {
			if (isLeft)
				host.setLeft(other, path);
			else
				host.setRight(other, path);
			
		}
		
		@Override
		public Node host() {
			return this.host;
		}
		
		@Override
		public boolean isLeft() {
			return this.isLeft;
		}
	}
	
	public EdgeClass getLeftEdge() {
		return new EdgeClass(this, true);
	}
	
	public EdgeClass getRightEdge() {
		return new EdgeClass(this, false);
	}
	
	public static String dumpShorten(String v) {
		if (v.length() > 10) {
			return String.format("%s...%d", v.substring(0, 10), v.length());
		} else
			return v;
	}
	
	@Override
	public String toString() {
		return toString(-1L);
	}

	public String toString(long snapshotId) {
		StringBuilder sb = new StringBuilder();
		byte[] pri = getPriority();
		String sKey;
		try {sKey = new String(getKey(), "UTF-8"); }
		catch (UnsupportedEncodingException ex) {sKey = "ERROR";}
		sb.append(String.format("[%d]%s %s", myId, sKey, getModified() ? "M" : ""));
		//sb.append(String.format(" pri=[%d,%d,%d,%d]", pri[0], pri[1], pri[2],pri[3]));
		sb.append(String.format(" s=%d%n", backingBuffer.size()));
		int c = getCountValue();
		int pos = getFirstValuePos();
		for(int i=0; i<c;i++) {
			long sid = backingBuffer.getLong(pos);
			pos += SIZE_OF_SNAPSHOT_ID;
			int len = backingBuffer.getUShort(pos);
			pos += SIZE_OF_SHORT;
			byte[] bvalue = new byte[len];
			backingBuffer.get(pos, bvalue);
			pos += len;
			String sValue;
			try {sValue = new String(bvalue, "UTF-8"); }
			catch (UnsupportedEncodingException ex) {sValue = "ERROR";}
			if (snapshotId == -1L || sid <= snapshotId)
				sb.append(String.format("v[%d]=S%d:'%s'",i, sid, dumpShorten(sValue)));
		}
		sb.append("\n");
		c = getCountLeft();
		pos = getFirstLeftPos();
		for(int i=0; i<c;i++) {
			long sid = backingBuffer.getLong(pos);
			pos += SIZE_OF_SNAPSHOT_ID;
			long nid = backingBuffer.getLong(pos);
			pos += SIZE_OF_NODE_ID;
			if (snapshotId == -1L || sid <= snapshotId)
				sb.append(String.format("L[%d]=S%d->%s ",i, sid, nid == Tree.TERMINAL_NODE_ID ? "NIL" : Long.toString(nid)));
		}
		sb.append("\n");
		c = getCountRight();
		pos = getFirstRightPos();
		for(int i=0; i<c;i++) {
			long sid = backingBuffer.getLong(pos);
			pos += SIZE_OF_SNAPSHOT_ID;
			long nid = backingBuffer.getLong(pos); 
			pos += SIZE_OF_NODE_ID;
			if (snapshotId == -1L || sid <= snapshotId)
				sb.append(String.format("R[%d]=S%d->%s ",i, sid, nid == Tree.TERMINAL_NODE_ID ? "NIL" : Long.toString(nid)));
		}
		sb.append("\n");
		sb.append(acm.toString(this, snapshotId));
		return sb.toString();
	}
}
