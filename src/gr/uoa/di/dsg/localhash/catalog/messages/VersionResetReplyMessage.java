package gr.uoa.di.dsg.localhash.catalog.messages;

public class VersionResetReplyMessage extends VersionResetMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3392198860471749741L;

	/**
	 * 
	 */

	public VersionResetReplyMessage(long snapshotId, byte[] authenticator) {
		super(snapshotId, authenticator);
	}

}
