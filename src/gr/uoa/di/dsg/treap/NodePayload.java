package gr.uoa.di.dsg.treap;

public class NodePayload {
	public long snapshotId;
	public byte[] value;
	
	public NodePayload(long snapshotId, byte[] value) {
		this.snapshotId = snapshotId;
		this.value = value;
	}
}
