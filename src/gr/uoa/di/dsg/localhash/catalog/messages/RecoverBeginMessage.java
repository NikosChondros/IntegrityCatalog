package gr.uoa.di.dsg.localhash.catalog.messages;

import gr.uoa.di.dsg.localhash.hostapp.Message;

public class RecoverBeginMessage extends Message {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2499530100777715942L;
	private long snapshotId;
	
	public RecoverBeginMessage(long snapshotId) {
		this.snapshotId = snapshotId;
	}
	
	public long getSnapshotId() {
		return snapshotId;
	}
	
	@Override
	protected String getArguments() {
		return String.format("sid=%d", snapshotId);
	}
}
