package gr.uoa.di.dsg.localhash.catalog;

import java.util.Arrays;

class SnapshotData {
	public long snapshotId;
	public byte[] authenticator;
	public SnapshotData(long snapshotId, byte[] authenticator) {
		this.snapshotId = snapshotId;
		this.authenticator = authenticator;
	}
	
	@Override
	public String toString() {
		return Long.toString(snapshotId) + "/" + Arrays.toString(authenticator);
	}
}