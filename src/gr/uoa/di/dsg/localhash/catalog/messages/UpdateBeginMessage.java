package gr.uoa.di.dsg.localhash.catalog.messages;

import gr.uoa.di.dsg.localhash.hostapp.Message;

public class UpdateBeginMessage extends Message {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5854073161914889504L;
	private long snapshotId;
	
	public UpdateBeginMessage(long snapshotId) {
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
