package gr.uoa.di.dsg.localhash.catalog.messages;

import gr.uoa.di.dsg.localhash.hostapp.Message;

public class RecoverDataMessage extends Message {
	/**
	 * 
	 */
	private static final long serialVersionUID = -909310999851482869L;
	private long snapshotId;
	private long blockNumber;
	private byte[] blockData;
	
	public RecoverDataMessage(long snapshotId, long blockNumber, byte[] blockData) {
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
		return String.format("sid=%d block-%d", snapshotId, blockNumber);
	}
}
