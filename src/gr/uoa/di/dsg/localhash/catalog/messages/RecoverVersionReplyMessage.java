package gr.uoa.di.dsg.localhash.catalog.messages;

import gr.uoa.di.dsg.localhash.catalog.ByteArrayWrapper;
import gr.uoa.di.dsg.localhash.hostapp.Message;

public class RecoverVersionReplyMessage extends Message {
	/**
	 * 
	 */
	private static final long serialVersionUID = 375876153967365473L;
	private long snapshotId;
	private byte[] authenticator;
	
	public RecoverVersionReplyMessage(long snapshotId, byte[] authenticator) {
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
