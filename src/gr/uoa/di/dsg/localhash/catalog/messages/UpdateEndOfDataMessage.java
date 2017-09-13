package gr.uoa.di.dsg.localhash.catalog.messages;

import gr.uoa.di.dsg.localhash.hostapp.Message;

public class UpdateEndOfDataMessage extends Message {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8777623004196476839L;
	private long snapshotId;
	
	public UpdateEndOfDataMessage(long snapshotId) {
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
