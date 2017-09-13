package gr.uoa.di.dsg.localhash.catalog.messages;

import gr.uoa.di.dsg.localhash.catalog.ByteArrayWrapper;
import gr.uoa.di.dsg.localhash.hostapp.Message;
import gr.uoa.di.dsg.localhash.hostapp.Node;

public class RemoteCatalogSyncMessage extends Message {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3387074246995113764L;
	private Node source;
	private long snapshotId;
	private byte[] authenticator;
	
	public RemoteCatalogSyncMessage(Node source, long snapshotId, byte[] authenticator) {
		this.source = source;
		this.snapshotId = snapshotId;
		this.authenticator = authenticator;
	}
	
	public byte[] getAuthenticator() {
		return authenticator;
	}
	
	public long getSnapshotId() {
		return snapshotId;
	}

	public Node getSource() {
		return source;
	}
	
	@Override
	protected String getArguments() {
		return String.format("sid=%d auth=%s", snapshotId, new ByteArrayWrapper(authenticator));
	}
}
