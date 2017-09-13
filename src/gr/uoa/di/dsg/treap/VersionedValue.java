package gr.uoa.di.dsg.treap;

import edu.stanford.identiscape.util.ByteArrayRegion;

public class VersionedValue {
	public long snapshotId;
	public ByteArrayRegion value;

	public VersionedValue(long snapshotId, ByteArrayRegion value) {
		this.snapshotId = snapshotId;
		this.value = value;
	}
	public VersionedValue(long snapshotId, byte[] value) {
		this.snapshotId = snapshotId;
		this.value = new ByteArrayRegion(value);
	}
}
