package gr.uoa.di.dsg.localhash.catalog.messages;

import gr.uoa.di.dsg.localhash.hostapp.Message;

public class RecoverEndOfDataMessage extends Message {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2874365494388161464L;
	private long snapshotId;
	
	public RecoverEndOfDataMessage(long snapshotId) {
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
