package gr.uoa.di.dsg.treap.direct;

import java.io.FileWriter;
import java.io.PrintWriter;

public class Tree implements TreeEdge {
	public static long TERMINAL_NODE_ID = -1;
	
	private NodeStorageManager storage;
	private int insertions = 0;
	private int rotations = 0;
	
	public Tree(NodeStorageManager storage) {
		this.storage = storage;
	}

	public void insert(byte[] key, byte[] value) throws Exception {
		rotations = 0;
		rinsert(key, value, this);
		insertions++;
	}
	
	private void rinsert(byte[] key, byte[] value, TreeEdge edge) throws Exception {
		Node current;
		long next = edge.follow();
		if( next == TERMINAL_NODE_ID ) {
			Node newNode = makeNode(key, value);
			edge.replace(newNode.getId());
			storage.update(newNode);
			return;
		} else
			current = storage.get(next);
		
		int i = current.compareKey(key);
		if( i > 0 ) { //key < current.key
			rinsert(key, value, current.getLeftEdge());
			if ( current.comparePriority(storage.get(current.getLeft())) < 0 )
				rotateRight(edge);
		} else if ( i < 0 ) { //newNode.key > current.key
			rinsert(key, value, current.getRightEdge());
			if ( current.comparePriority(storage.get(current.getRight())) < 0 )
				rotateLeft(edge);
		} else
			throw new IllegalArgumentException("Entry already in dictionary");
	}
	
	public byte[] get(byte[] key) {
		return rsearch(key, storage.getRoot());
	}
	
	private byte[] rsearch(byte[] key, long nodeId) {
		if( nodeId == TERMINAL_NODE_ID )
			return null;
		Node node = storage.get(nodeId);
		int i = node.compareKey(key);
		if( i == 0 )
			return node.getValue();
		if( i > 0 )
			return rsearch(key, node.getLeft());
		else
			return rsearch(key, node.getRight());
	}
	
	public byte[] update(byte[] key, byte[] value) {
		return rupdate(key, value, storage.getRoot());
	}
	
	private byte[] rupdate(byte[] key, byte[] value, long nodeId) {
		Node node = storage.get(nodeId);
		if( node == null )
			return null;
		int i = node.compareKey(key);
		if( i == 0 ) {
			byte[] ret = node.getValue();
			node.setValue(value);
			return ret;
		}
		if( i > 0 )
			return rsearch(key, node.getLeft());
		else
			return rsearch(key, node.getRight());
	}
	
	private void testdump(String label) {
		String filename = String.format("treap-i%02d-r%02d-%s.dot", insertions, rotations, label);
		try {
			dump(filename);
			Runtime.getRuntime().exec("dot2ps " + filename);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	private void rotateLeft(TreeEdge incomingEdge) {
		testdump("l-apre");
		
		/*
		  procedure ROTATE-LEFT( T: treap )
		  	[T,T->rchild,T->rchild->lchild] = [T->rchild,T->rchild->lchild,T]
		*/
	  	Node current = storage.get(incomingEdge.follow());
		Node myRight = storage.get(current.getRight());
	  	long itsLeft = myRight.getLeft();
	  	
	  	/*
	  	 * myRight is promoted (pointed to by incoming edge), gets the current node as its left child (old=replaced one) and the current node gets the old as its right child
	  	 */
	  	incomingEdge.replace(myRight.getId());
	  	myRight.setLeft(current.getId());
	  	current.setRight(itsLeft);
		testdump("l-bpost");
		rotations++;
	}
	
	private void rotateRight(TreeEdge incomingEdge) {
		testdump("r-apre");
		/*
		  procedure ROTATE-RIGHT( T: treap )
		  	[T,T->lchild,T->lchild->rchild] = [T->lchild,T->lchild->rchild,T]
		*/
	  	Node current = storage.get(incomingEdge.follow());
		Node myLeft = storage.get(current.getLeft());
	  	long itsRight = myLeft.getRight();
	  	
	  	/*
	  	 * myLeft is promoted (pointed to by incoming edge), gets the current node as its right child (old=replaced one) and the current node gets the old as its left child
	  	 */
	  	incomingEdge.replace(myLeft.getId());
	  	myLeft.setRight(current.getId());
	  	current.setLeft(itsRight);
		testdump("r-bpost");
		rotations++;
	}

	private Node makeNode(byte[] key, byte[] value) {
		return new Node(storage.nextId(), key, value);
	}
	
	@Override
	public long follow() {
		return storage.getRoot();
	}
	
	@Override
	public void replace(long newRoot) {
		storage.setRoot(newRoot);
	}
	
	public void dump(String filename) throws Exception {
		PrintWriter dot = new PrintWriter(new FileWriter(filename));
		dot.println("digraph Treap {");
		dot.println("graph [ordering=\"out\"];");
		rdump(dot, storage.getRoot(), TERMINAL_NODE_ID, "");
		dot.println("}");
		dot.close();
	}
	
	private void rdump(PrintWriter dot, long nodeId, long parentId, String tag) throws Exception {
		String sId, sLabel;
		Node node = null;
		if( nodeId == TERMINAL_NODE_ID ) {
			sId = String.format("N%d%sNIL", parentId, tag);
			sLabel = "NIL";
		} else {
			node = storage.get(nodeId);
			sId = String.format("N%d", nodeId);
			byte[] pri = node.getPriority();
			sLabel = String.format("%s:%s pri=[%d,%d,%d,%d]", new String(node.getKey(), "UTF-8"), new String(node.getValue(), "UTF-8"), pri[0], pri[1], pri[2],pri[3]);
		}
			
		dot.format("  %s [label=\"%s\"];%n", 
				sId, sLabel);
		if( parentId != TERMINAL_NODE_ID )
			dot.format(" N%d -> %s%n", parentId, sId);
		
		if( node != null ) {
			long left = node.getLeft();
			long right = node.getRight();
			if( ! ( left == TERMINAL_NODE_ID && right == TERMINAL_NODE_ID ) ) {
				rdump(dot, left, nodeId, "L");
				rdump(dot, right, nodeId, "R");
			}
		}
	}
}
