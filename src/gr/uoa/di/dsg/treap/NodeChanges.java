package gr.uoa.di.dsg.treap;

public class NodeChanges {
	public boolean isAppend;
	public byte[] key;
	public byte[] value;
	
	public NodeChanges(boolean isAppend, byte[] key, byte[] value) {
		this.isAppend = isAppend;
		this.key = key;
		this.value = value;
	}
}
