package gr.uoa.di.dsg.localhash.catalog.messages;

import gr.uoa.di.dsg.localhash.catalog.ByteArrayWrapper;
import gr.uoa.di.dsg.localhash.hostapp.Message;

public class VersionResetMessage extends Message {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6108804875742089632L;
	/**
	 * 
	 */
	private long snapshotId;
	private byte[] authenticator;
	
	public VersionResetMessage(long snapshotId, byte[] authenticator) {
		this.snapshotId = snapshotId;
		this.authenticator = authenticator;
	}
	
	public byte[] getAuthenticator() {
		return authenticator;
	}
	
	public long getSnapshotId() {
		return snapshotId;
	}
	
	@Override
	protected String getArguments() {
		return String.format("sid=%d auth=%s", snapshotId, new ByteArrayWrapper(authenticator));
	}
}
