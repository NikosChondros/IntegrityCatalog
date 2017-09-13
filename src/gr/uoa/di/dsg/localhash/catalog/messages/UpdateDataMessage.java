package gr.uoa.di.dsg.localhash.catalog.messages;

import gr.uoa.di.dsg.localhash.hostapp.Message;

public class UpdateDataMessage extends Message {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5454654129530048674L;
	private long snapshotId;
	private long blockNumber;
	private byte[] blockData;
	
	public UpdateDataMessage(long snapshotId, long blockNumber, byte[] blockData) {
		this.snapshotId = snapshotId;
		this.blockNumber = blockNumber;
		this.blockData = blockData;
	}
	
	public long getSnapshotId() {
		return snapshotId;
	}

	public long getBlockNumber() {
		return blockNumber;
	}

	public byte[] getBlockData() {
		return blockData;
	}
	
	@Override
	protected String getArguments() {
		return String.format("sid=%d block=%d", snapshotId, blockNumber);
	}
}
