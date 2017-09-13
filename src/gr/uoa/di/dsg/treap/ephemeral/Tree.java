package gr.uoa.di.dsg.treap.ephemeral;

import java.io.FileWriter;
import java.io.PrintWriter;

public class Tree implements TreeEdge {
	public static long TERMINAL_NODE_ID = -1;
	
	private NodeStorageManager storage;
	
	public Tree(NodeStorageManager storage) {
		this.storage = storage;
	}

	private Node makeNode(byte[] key, byte[] value) {
		byte[] buffer = new byte[Node.estimateNewNodeSize(key, value)];
		return new Node(storage.nextId(), buffer, key, value);
	}
	
	public void insert(byte[] key, byte[] value) throws Exception {
		rinsert(key, value, this);
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
			storage.update(node);
			return ret;
		}
		if( i > 0 )
			return rupdate(key, value, node.getLeft());
		else
			return rupdate(key, value, node.getRight());
	}
	
	private void rotateLeft(TreeEdge incomingEdge) {
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
	}
	
	private void rotateRight(TreeEdge incomingEdge) {
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
