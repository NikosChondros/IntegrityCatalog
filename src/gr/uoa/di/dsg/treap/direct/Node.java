package gr.uoa.di.dsg.treap.direct;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Node {
	private long id;
	private byte[] key;
	private byte[] value;
	private byte[] priority;
	
	private long left;
	private long right;
	
	private static final MessageDigest hasher;
	static {
		try {
			hasher = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException ex) {
			throw new RuntimeException("Error in static initializer for Node MessageDigest", ex);
		}
	}

	public long getId() {
		return id;
	}
	
	public long getLeft() {
		return left;
	}
	
	public long getRight() {
		return right;
	}
	
	public void setLeft(long left) {
		this.left = left;
	}
	
	public void setRight(long right) {
		this.right = right;
	}
	
	public class EdgeClass implements TreeEdge {
		boolean isLeft;
		Node host;
		
		
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
		public void replace(long other) {
			if (isLeft)
				host.setLeft(other);
			else
				host.setRight(other);
			
		}
	}
	
	public EdgeClass getLeftEdge() {
		return new EdgeClass(this, true);
	}
	
	public EdgeClass getRightEdge() {
		return new EdgeClass(this, false);
	}
	
	public byte[] getKey() {
		return key;
	}
	
	public byte[] getValue() {
		return value;
	}
	
	public void setValue(byte[] value) {
		this.value = value;
	}
	public byte[] getPriority() {
		return priority;
	}
	
	public Node(long id, byte[] key, byte[] value) {
		this.id = id;
		this.key = key;
		this.value = value;
		hasher.reset();
		hasher.update(key);
		this.priority = hasher.digest();
		left = right = Tree.TERMINAL_NODE_ID;
	}
	
	public int compareKey(Node other) {
		return compare(key, other.key);
	}

	public int compareKey(byte[] other) {
		return compare(key, other);
	}

	public int comparePriority(Node other) {
		return compare(priority, other.priority);
	}

	public static int compare(byte[] first, byte[] second) {
		int ret, minLength;
		if( first.length == second.length ) {
			ret = 0;
			minLength = first.length;
		} else if (first.length < second.length ) {
			ret = -1;
			minLength = first.length;
		} else {
			ret = 1;
			minLength = second.length;
		}
		
		for (int i = 0; i < minLength; i++)
			if (first[i] < second[i])
				return -1; // it's less
			else if (first[i] > second[i])
				return 1; // it's greater
		return ret; // all were equal so far, return result based on length
	}
}
