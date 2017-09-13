package gr.uoa.di.dsg.treap;

import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.function.Consumer;

import edu.stanford.identiscape.util.ByteArrayRegion;
import edu.stanford.identiscape.util.Bytes;
import gr.uoa.di.dsg.treap.SnapshotExtractorFunctions.SnapshotExtractorState;

public class Tree implements Closeable {
	public static long TERMINAL_NODE_ID = -1;
	public boolean debugSnapshot;
	public boolean debugIsMember;
	private NodeStorageManager storage;
	private AuthenticatorCacheManager acm;
	private ISnapshotExtractor se;
	private int maxNodeSize;
	private int nodeCopies = 0;
	private ByteArrayRegion workKey = new ByteArrayRegion(null, 0, 0);
	private ByteArrayRegion workValue = new ByteArrayRegion(null, 0, 0);
	private HashMap<Long, ISnapshotExtractorInstance> pendingSnapshotExtractorInstances = new HashMap<>();
	
	public Tree(NodeStorageManager storage, AuthenticatorCacheManager acm) {
		this.storage = storage;
		this.acm = acm;
		this.se = null; //new SnapshotExtractorDebug();
		this.maxNodeSize = storage.getMaxNodeSize();
	}
	
	public Tree(NodeStorageManager storage, AuthenticatorCacheManager acm, ISnapshotExtractor se) {
		this.storage = storage;
		this.acm = acm;
		this.se = se;
		this.maxNodeSize = storage.getMaxNodeSize();
	}
	
	@Override
	public void close() throws IOException {
		for(ISnapshotExtractorInstance i: pendingSnapshotExtractorInstances.values())
			i.finishOutput();
		pendingSnapshotExtractorInstances.clear();
		this.storage.close();
	}
	
	public NodeStorageManager getStorageManager() {
		return storage;
	}
	
	protected AuthenticatorCacheManager getACM() {
		return acm;
	}

	public final int getMaxNodeSize() {
		return maxNodeSize;
	}
	
	public final void incrNodeCopies() {
		nodeCopies += 1;
	}
	
	public final int getNodeCopies() {
		return nodeCopies;
	}

	protected Node createNode(ByteArrayRegion key, VersionedValue vvalue, long left, long right) throws IOException {
		return storage.create(this, key, vvalue, left, right);
	}
	
	protected Node fetchNode(long nodeId) throws IOException {
		//MemoryBuffer buffer = storage.get(nodeId);
		//return new Node(this, acm, nodeId, buffer);
		return storage.fetch(nodeId, this, acm);
	}
	
	protected Node releaseNode(Node node) {
		storage.release(node);
		return null;
	}
	
	protected void markDirty(Node node) throws IOException {
		storage.markDirty(node);
	}

	public void insert(byte[] key, byte[] value) throws Exception {
		workKey.set(key);
		workValue.set(value);
		insert(workKey, workValue);
	}
	
	public void insert(ByteArrayRegion key, ByteArrayRegion value) throws Exception {
		storage.beginWork();
		rinsert(key, value, new TreePath(new RootEdge(storage)));
		storage.endWork();
	}
	
	private void rinsert(ByteArrayRegion key, ByteArrayRegion value, TreePath path) throws Exception {
		TreeEdge edge = path.last();
		Node current;
		long next = edge.follow();
		if( next == TERMINAL_NODE_ID ) {
			Node newNode = createNode(key, new VersionedValue(getCurrentSnapshotId(), value), TERMINAL_NODE_ID, TERMINAL_NODE_ID);
			edge.replace(newNode.getId(), path);
			newNode = releaseNode(newNode);
		} else {
			current = fetchNode(next);
			int currentVSkey = current.compareKey(key);
			if( currentVSkey > 0 ) { //key < current.key, go left
				path.add(current.getLeftEdge()); //augment path with the edge I am following to descend
				rinsert(key, value, path);
				path.removeLast();
				next = path.last().follow();
				if (next != current.getId()) {
					current = releaseNode(current); //release our loaded node because it was modified (copied over) in the recursive call
					current = fetchNode(next);
				}
				Node nextForRotate = fetchNode(current.getLeft());
				if ( current.comparePriority(nextForRotate) < 0 )
					rotateRight(path); //rotate will mark the node dirty in any case, no need to repeat it
				else
					current.markModified(); //mark it as modified for authenticator recalculation
				nextForRotate = releaseNode(nextForRotate);
			} else if ( currentVSkey < 0 ) { //newNode.key > current.key
				path.add(current.getRightEdge()); //augment path with the edge I am following to descend
				rinsert(key, value, path);
				path.removeLast();
				next = path.last().follow();
				if (next != current.getId()) {
					current = releaseNode(current); //release our loaded node because it was modified (copied over) in the recursive call
					current = fetchNode(next);
				}
				Node nextForRotate = fetchNode(current.getRight());
				if ( current.comparePriority(nextForRotate) < 0 )
					rotateLeft(path); //rotate will mark the node dirty in any case, no need to repeat it
				else
					current.markModified(); //mark it modified for authenticator recalc
				nextForRotate = releaseNode(nextForRotate);
			} else
				throw new IllegalArgumentException("Entry already in dictionary");
			current = releaseNode(current);
		}
	}
	
	private void rotateRight(TreePath path) throws IOException {
		/*
		  procedure ROTATE-RIGHT( T: treap )
		  	[T,T->lchild,T->lchild->rchild] = [T->lchild,T->lchild->rchild,T]
		*/
		TreeEdge incomingEdge = path.last();
	  	Node current = fetchNode(incomingEdge.follow());
		Node myLeft = fetchNode(current.getLeft());
	  	long itsRight = myLeft.getRight();
	  	
	  	/*
	  	 * myLeft is promoted (pointed to by incoming edge), gets the current node as its right child (old=replaced one) and the current node gets the old as its left child
	  	 */
	  	incomingEdge.replace(myLeft.getId(), path);
	  	myLeft.setRight(current.getId(), path);
	  	current.setLeft(itsRight, path);
	  	current = releaseNode(current);
	  	myLeft = releaseNode(myLeft);
	  	//we don't need to update path with the rotation because all changes happen from the last node and downwards (at nodes beyond the current path) 
	}
	
	private void rotateLeft(TreePath path) throws IOException {
		/*
		  procedure ROTATE-LEFT( T: treap )
		  	[T,T->rchild,T->rchild->lchild] = [T->rchild,T->rchild->lchild,T]
		*/
		TreeEdge incomingEdge = path.last();
	  	Node current = fetchNode(incomingEdge.follow());
		Node myRight = fetchNode(current.getRight());
	  	long itsLeft = myRight.getLeft();
	  	
	  	/*
	  	 * myRight is promoted (pointed to by incoming edge), gets the current node as its left child (old=replaced one) and the current node gets the old as its right child
	  	 */
	  	incomingEdge.replace(myRight.getId(), path);
	  	myRight.setLeft(current.getId(), path);
	  	current.setRight(itsLeft, path);
	  	current = releaseNode(current);
	  	myRight = releaseNode(myRight);
	  	//we don't need to update path with the rotation because all changes happen from the last node and downwards (at nodes beyond the current path) 
	}
	
	public byte[] get(byte[] key) throws IOException {
		storage.beginWork();
		byte[] ret = rsearch(key, storage.getRoot());
		storage.endWork();
		return ret;
	}
	
	private byte[] rsearch(byte[] key, long nodeId) throws IOException {
		byte[] ret;
		if( nodeId == TERMINAL_NODE_ID )
			return null;
		Node node = fetchNode(nodeId);
		int i = node.compareKey(key);
		if( i == 0 ) {
			ret = node.getValue();
			node = releaseNode(node);
		} else {
			if( i > 0 ) {
				long next = node.getLeft();
				node = releaseNode(node);
				ret = rsearch(key, next);
			} else {
				long next = node.getRight();
				node = releaseNode(node);
				ret = rsearch(key, next);
			}
		}
		return ret;
	}
	
	public byte[] get(long snapshotId, byte[] key) throws IOException {
		storage.beginWork();
		byte[] ret = rsearchHistorical(snapshotId, key, storage.getRoot(snapshotId));
		storage.endWork();
		return ret;
	}
	
	private byte[] rsearchHistorical(long snapshotId, byte[] key, long nodeId) throws IOException {
		byte[] ret;
		if( nodeId == TERMINAL_NODE_ID )
			return null;
		Node node = fetchNode(nodeId);
		int i = node.compareKey(key);
		if( i == 0 ) {
			ret = node.getValue(snapshotId);
			node = releaseNode(node);
		} else {
			if( i > 0 ) {
				long next = node.getLeft(snapshotId);
				node = releaseNode(node);
				ret = rsearchHistorical(snapshotId, key, next);
			} else {
				long next = node.getRight(snapshotId);
				node = releaseNode(node);
				ret = rsearchHistorical(snapshotId, key, next);
			}
		}
		return ret;
	}
	
	public ExistenceProof isMember(long snapshotId, byte[] key) throws IOException {
		workKey.set(key);
		return isMember(snapshotId, workKey);
	}
	
	public ExistenceProof isMember(long snapshotId, ByteArrayRegion key) throws IOException {
		storage.beginWork();

		ExistenceProof.reset();
		long root = storage.getRoot(snapshotId);
		if( root == TERMINAL_NODE_ID )
			ExistenceProof.decide(false); // will result in a completely empty proof
		else
			risMember(snapshotId, key, root);
		storage.endWork();
		return ExistenceProof.conclude();
	}
	
	private void risMember(long snapshotId, ByteArrayRegion key, long nodeId) throws IOException {
		if( nodeId == TERMINAL_NODE_ID ) {
			ExistenceProof.feed(Node.AUTHENTICATOR_OF_TERMINAL_NODE); //this is required to derive the root authenticator, proving all is well in the not found case
			ExistenceProof.decide(false);
		} else {
			Node node = fetchNode(nodeId);
			ExistenceProof.feed(node, snapshotId);
			int result = node.compareKey(key);
			long left = node.getLeft(snapshotId);
			long right = node.getRight(snapshotId);
			node = releaseNode(node);
			if( result == 0 ) {
				if( debugIsMember )
					System.out.format("Membership recurse into %d(%d,%d) found%n", nodeId, left, right);
				//bingo, found!
				ExistenceProof.feed(getAuthenticator(snapshotId, left));
				ExistenceProof.feed(getAuthenticator(snapshotId, right));
				ExistenceProof.decide(true);
			} else {
				if( result > 0 ) { // key < node, going left
					if( debugIsMember )
						System.out.format("Membership recurse into %d(%d,%d) going left%n", nodeId, left, right);
					ExistenceProof.feed(getAuthenticator(snapshotId, right));
					risMember(snapshotId, key, left);
				} else { // key > node, going right
					if( debugIsMember )
						System.out.format("Membership recurse into %d(%d,%d) going right%n", nodeId, left, right);
					ExistenceProof.feed(getAuthenticator(snapshotId, left));
					risMember(snapshotId, key, right);
				}
			}
		}
	}
	
	public byte[] update(byte[] key, byte[] value) throws IOException {
		storage.beginWork();
		byte[] ret = rupdate(key, value, new TreePath(new RootEdge(storage)));
		storage.endWork();
		return ret;
	}
	
	private byte[] rupdate(byte[] key, byte[] value, TreePath path) throws IOException {
		long nodeId = path.last().follow();
		Node node = fetchNode(nodeId);
		if( node == null )
			return null;
		int i = node.compareKey(key);
		if( i == 0 ) {
			byte[] ret = node.getValue();
			node.setValue(value, path);
			return ret;
		}
		byte[] ret;
		if( i > 0 ) {
			path.add(node.getLeftEdge());
			ret = rupdate(key, value, path);
			path.removeLast();
		} else {
			path.add(node.getRightEdge());
			ret = rupdate(key, value, path);
			path.removeLast();
		}
		if( ret != null )
			node.markModified(); //make sure all the nodes on the path to the root are marked dirty on update of a child
		node = releaseNode(node);
		return ret;
	}
	
	public byte[] closeSnapshot() throws IOException, InterruptedException {
		storage.beginWork();
		Consumer<Node> changesRecorder;
		Runnable seiEnd;
		if (se != null) {
			final ISnapshotExtractorInstance sei = se.begin(getCurrentSnapshotId());
			changesRecorder = (Node n) -> {sei.process(n);};
			seiEnd = () -> sei.end();
			pendingSnapshotExtractorInstances.put(getCurrentSnapshotId(), sei);
		} else {
			changesRecorder = (Node n) -> {};
			seiEnd = () -> {};
		}
		
		//long rootNode = storage.getRoot();
		byte[] rootAuthenticator = produceAuthenticator(new TreePath(new RootEdge(storage)), changesRecorder);
		//at this point, root may have been replaced by a new copy
		seiEnd.run();
		storage.closeSnapshot(storage.getRoot(), rootAuthenticator);
		storage.endWork();
		return rootAuthenticator;
	}
	
	protected byte[] produceAuthenticator(TreePath path, Consumer<Node> changesRecorder) throws IOException {
		byte[] ret;
		long nodeId = path.last().follow();
		if( nodeId == TERMINAL_NODE_ID )
			ret = Node.AUTHENTICATOR_OF_TERMINAL_NODE;
		else {
			if( debugSnapshot )
				System.out.format("Snapshot, recurse in d=%d id=%d%n", path.depth(), nodeId);
			Node node = fetchNode(nodeId); 
			ret = node.getCurrentAuthenticator(path, changesRecorder); //this one may modify the node if it was already dirty
			node = releaseNode(node);
			if( debugSnapshot )
				System.out.format("Snapshot, recurse out d=%d id=%d auth=%s%n", path.depth(), nodeId, Bytes.toString(ret));
		}
		//*DEBUG*/ System.out.format("produceAuthenticator: %d=%s%n", nodeId, Bytes.toString(ret));
		return ret;
	}
	
	public long getRoot(long snapshotId) throws IOException {
		return storage.getRoot(snapshotId);
	}
	
	public byte[] getRootAuthenticator(long snapshotId) throws IOException {
		return getAuthenticator(snapshotId, storage.getRoot(snapshotId));
	}
	
	protected byte[] getAuthenticator(long snapshotId, long nodeId) throws IOException {
		byte[] ret;
		if( nodeId == TERMINAL_NODE_ID )
			ret = Node.AUTHENTICATOR_OF_TERMINAL_NODE;
		else {
			Node node = fetchNode(nodeId); 
			ret = node.getAuthenticator(snapshotId); //this one should never modify the node
			node = releaseNode(node);
		}
		//*DEBUG*/ System.out.format("getAuthenticator(%d): %d=%s%n", snapshotId, nodeId, Bytes.toString(ret));
		return ret;
	}
	
	public int getLargestNodeSize() {
		return storage.getLargestNodeSize();
	}
	
	public void extractSnapshot(String extractBaseFileName, long snapshotId) throws IOException {
		SnapshotExtractorState state = new SnapshotExtractorState(SnapshotExtractorFunctions.fileNameOfExtract(extractBaseFileName, snapshotId));
		storage.beginWork();
		rExtractSnapshot(snapshotId, storage.getRoot(snapshotId), state);
		storage.endWork();
		state.discard();
	}
	
	private void rExtractSnapshot(long snapshotId, long nodeId, SnapshotExtractorState state) throws IOException {
		Node node = null;
		if( nodeId != TERMINAL_NODE_ID ) {
			node = fetchNode(nodeId);
			NodeChanges nc = node.getNodeChanges(snapshotId);
			if (nc != null) {
				SnapshotExtractorFunctions.writeNodeChangesToFile(nc, state);
			}
			long left = node.getLeft(snapshotId);
			long right = node.getRight(snapshotId);
			node = releaseNode(node);
			if ( left != TERMINAL_NODE_ID )
				rExtractSnapshot(snapshotId, left, state);
			if ( right != TERMINAL_NODE_ID )
				rExtractSnapshot(snapshotId, right, state);
		}
		
	}
	
	public void dump(String filename) throws Exception {
		PrintWriter dot = new PrintWriter(new FileWriter(filename));
		dot.format("digraph \"%s\" {%n", filename);
		dot.println("graph [ordering=\"out\"];");
		rdump(dot, storage.getRoot(), TERMINAL_NODE_ID, "", -1L);
		dot.println("}");
		dot.close();
	}
	
	public void dump(String filename, long snapshotId) throws Exception {
		PrintWriter dot = new PrintWriter(new FileWriter(filename));
		dot.format("digraph \"%s\" {%n", filename);
		dot.println("graph [ordering=\"out\"];");
		rdump(dot, storage.getRoot(snapshotId), TERMINAL_NODE_ID, "", snapshotId);
		dot.println("}");
		dot.close();
	}
	
	private void rdump(PrintWriter dot, long nodeId, long parentId, String tag, long snapshotId) throws Exception {
		String sId, sLabel;
		Node node = null;
		if( nodeId == TERMINAL_NODE_ID ) {
			sId = String.format("N%d%sNIL", parentId, tag);
			sLabel = "NIL";
		} else {
			node = fetchNode(nodeId);
			sId = String.format("N%d", nodeId);
//			byte[] pri = node.getPriority();
//			if( snapshotId > 0L )
//				sLabel = String.format("[%d]%s@%d:%s %spri=[%d,%d,%d,%d] ca=%d", 
//						nodeId, new String(node.getKey(), "UTF-8"), node.getOriginalSnapshot(), 
//						Node.dumpShorten( new String( snapshotId > 0 ? node.getValue(snapshotId) :node.getValue(), "UTF-8") ),
//						node.getModified() ? "M " : "",
//						pri[0], pri[1], pri[2],pri[3], node.getCountAuthenticator());
//			else
//				sLabel = node.toString();
			sLabel = node.toString(snapshotId);
		}
			
		dot.format("  %s [label=\"%s\"];%n", 
				sId, sLabel.replace("\n", "\\n"));
		if( parentId != TERMINAL_NODE_ID )
			dot.format(" N%d -> %s%n", parentId, sId);
		
		if( node != null ) {
			long left = snapshotId > 0 ? node.getLeft(snapshotId) : node.getLeft();
			long right = snapshotId > 0 ? node.getRight(snapshotId) : node.getRight();
			if( ! ( left == TERMINAL_NODE_ID && right == TERMINAL_NODE_ID ) ) {
				rdump(dot, left, nodeId, "L", snapshotId);
				rdump(dot, right, nodeId, "R", snapshotId);
			}
			node = releaseNode(node);
		}
	}
	
	public long getCurrentSnapshotId() {
		return storage.getCurrentSnapshotId();
	}
	
	public long getLastClosedSnapshotId() {
		return storage.getCurrentSnapshotId() - 1L;
	}
	
	private class RootEdge implements TreeEdge {
		private NodeStorageManager storage;
		public RootEdge(NodeStorageManager storage) {
			this.storage = storage;
		}
		@Override
		public long follow() {
			return storage.getRoot();
		}
		
		@Override
		public void replace(long newRoot, TreePath path) throws IOException {
			storage.setRoot(newRoot);
		}
		
		@Override
		public Node host() {
			return null;
		}
		
		@Override
		public boolean isLeft() {
			return false;
		}
	}
}
