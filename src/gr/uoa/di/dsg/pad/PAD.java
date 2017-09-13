package gr.uoa.di.dsg.pad;

public abstract class PAD {
	public abstract void insert(byte[] key, byte[] value);
	public abstract void update(byte[] key, byte[] value);
	public abstract ExistenceProof prove(byte[] key);
	public abstract void close();
	
	public abstract byte[] getAuthenticator(long snapshotId);
	public abstract void closeSnapshot();
	public abstract long getLastClosedSnapshotId();
	
	/*Missing binary update methods for now*/
}
