package gr.uoa.di.dsg.localhash.catalog.messages;

import gr.uoa.di.dsg.localhash.hostapp.Message;

public class RecoverGetNextMessage extends Message {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1017856200388801386L;
	private long snapshotId;
	private long blockNumber;
	
	public RecoverGetNextMessage(long snapshotId, long blockNumber) {
		this.snapshotId = snapshotId;
		this.blockNumber = blockNumber;
	}
	
	public long getSnapshotId() {
		return snapshotId;
	}

	public long getBlockNumber() {
		return blockNumber;
	}
	
	@Override
	protected String getArguments() {
		return String.format("sid=%d block=%d", snapshotId, blockNumber);
	}
}
