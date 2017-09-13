package gr.uoa.di.dsg.treap.ephemeral;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Node {
	private static final MessageDigest hasher;
	private static final int DIGEST_SIZE;
	static {
		try {
			hasher = MessageDigest.getInstance("SHA-1");
			DIGEST_SIZE = hasher.getDigestLength();
		} catch (NoSuchAlgorithmException ex) {
			throw new RuntimeException("Error in static initializer for Node MessageDigest", ex);
		}
	}
	
	public static final int SHORT_SIZE=Short.SIZE / Byte.SIZE;
	public static final int LONG_SIZE=Long.SIZE / Byte.SIZE;
	
	public static final int LEFT_START = 0;
	public static final int LEFT_SIZE = LONG_SIZE;
	public static final int RIGHT_START = LEFT_START+LEFT_SIZE;
	public static final int RIGHT_SIZE = LONG_SIZE;
	public static final int PRIORITY_START = RIGHT_START + RIGHT_SIZE;
	public static final int PRIORITY_SIZE = DIGEST_SIZE;
	
	public static final int KEY_LENGTH_START = PRIORITY_START + PRIORITY_SIZE;
	public static final int KEY_LENGTH_SIZE = SHORT_SIZE;
	public static final int VALUE_LENGTH_START = KEY_LENGTH_START + KEY_LENGTH_SIZE;
	public static final int VALUE_LENGTH_SIZE = SHORT_SIZE;
	
	public static final int FIXED_SIZE = VALUE_LENGTH_START + VALUE_LENGTH_SIZE;
	public static final int KEY_START = FIXED_SIZE;
	
	private long id;
	private ByteBuffer backingBuffer;
	
	public static int estimateNewNodeSize(byte[] key, byte[] value) {
		return key.length + value.length + FIXED_SIZE;
	}

	public Node(long id, byte[] buffer, byte[] key, byte[] value) {
		this.id = id;
		this.backingBuffer = ByteBuffer.wrap(buffer);
		setKeyLength(key.length);
		setValueLength(value.length);
		setKeyRaw(key);
		setValueRaw(value);
		hasher.reset();
		hasher.update(key);
		setPriority(hasher.digest());
		
		setLeft(Tree.TERMINAL_NODE_ID);
		setRight(Tree.TERMINAL_NODE_ID);
	}
	
	public Node(long id, byte[] buffer) {
		this.id = id;
		this.backingBuffer = ByteBuffer.wrap(buffer);
	}

	protected byte[] getBuffer() {
		return backingBuffer.array();
	}
	
	private static final short int2short(int v) {
		return (short) (v & 0xffff);
	}
	
	private static final int short2int(short v) {
		return (int) v & 0xFFFF;
	}
	
	public long getId() {
		return id;
	}
	
	private int getKeyLength() {
		return short2int(backingBuffer.getShort(KEY_LENGTH_START));
	}
	
	private void setKeyLength(int v) {
		backingBuffer.putShort(KEY_LENGTH_START, int2short(v));
	}
	
	private int getValueLength() {
		return short2int(backingBuffer.getShort(VALUE_LENGTH_START));
	}
	
	private void setValueLength(int v) {
		backingBuffer.putShort(VALUE_LENGTH_START, int2short(v));
	}
	
	private final int getKeyStart() {
		return KEY_START;
	}
	
	private final int getValueStart() {
		return KEY_START + getKeyLength();
	}
	
	public long getLeft() {
		return backingBuffer.getLong(LEFT_START);
	}
	
	public void setLeft(long left) {
		backingBuffer.putLong(LEFT_START, left);
	}
	
	public long getRight() {
		return backingBuffer.getLong(RIGHT_START);
	}
	
	public void setRight(long right) {
		backingBuffer.putLong(RIGHT_START, right);
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
		byte[] ret = new byte[getKeyLength()]; 
		backingBuffer.position(getKeyStart());
		backingBuffer.get(ret);
		return ret;
	}
	
	private void setKeyRaw(byte[] key) {
		backingBuffer.position(getKeyStart());
		backingBuffer.put(key);
	}

	public byte[] getValue() {
		byte[] ret = new byte[getValueLength()]; 
		backingBuffer.position(getValueStart());
		backingBuffer.get(ret);
		return ret;
	}
	
	private void setValueRaw(byte[] value) {
		backingBuffer.position(getValueStart());
		backingBuffer.put(value);
	}
	
	public void setValue(byte[] value) {
		//*TODO
		int currentLength = getValueLength();
		int diff = value.length - currentLength;
		if ( diff == 0 ) {
			setValueRaw(value);
		} else {
			int currentBufferLength = backingBuffer.capacity(); 
			byte[] copy = new byte[currentBufferLength + diff];
			if ( diff < 0 ) {
				System.arraycopy(backingBuffer.array(), 0, copy, 0, currentBufferLength + diff);
			} else {
				//copy
				System.arraycopy(backingBuffer.array(), 0, copy, 0, currentBufferLength);
			}
			backingBuffer = ByteBuffer.wrap(copy);
			setValueLength(value.length);
			setValueRaw(value);
		}
	}
	
	public byte[] getPriority() {
		byte[] ret = new byte[PRIORITY_SIZE]; 
		backingBuffer.position(PRIORITY_START);
		backingBuffer.get(ret);
		return ret;
	}
	
	private void setPriority(byte[] value) {
		backingBuffer.position(PRIORITY_START);
		backingBuffer.put(value);
	}
	
	public int compareKey(byte[] other) {
		return compare(backingBuffer.array(), getKeyStart(), getKeyLength(), other, 0, other.length);
	}

	public int comparePriority(Node other) {
		return compare(backingBuffer.array(), PRIORITY_START, PRIORITY_SIZE, other.backingBuffer.array(), PRIORITY_START, PRIORITY_SIZE);
	}

	public static int compare(byte[] firstData, int firstStart, int firstLength, byte[] secondData, int secondstart, int secondLength) {
		int ret, minLength;
		if( firstLength == secondLength ) {
			ret = 0;
			minLength = firstLength;
		} else if (firstLength < secondLength ) {
			ret = -1;
			minLength = firstLength;
		} else {
			ret = 1;
			minLength = secondLength;
		}
		
		for (int i = 0; i < minLength; i++)
			if (firstData[firstStart + i] < secondData[secondstart + i])
				return -1; // it's less
			else if (firstData[firstStart + i] > secondData[secondstart + i])
				return 1; // it's greater
		return ret; // all were equal so far, return result based on length
	}
}
