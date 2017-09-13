package gr.uoa.di.dsg.localhash.catalog.messages;

public class StoreReplyMessage extends StoreMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6565655258974071661L;

	public StoreReplyMessage(long snapshotId, byte[] authenticator) {
		super(snapshotId, authenticator);
	}

}
