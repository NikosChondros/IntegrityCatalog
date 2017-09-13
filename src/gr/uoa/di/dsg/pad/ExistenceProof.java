package gr.uoa.di.dsg.pad;

public abstract class ExistenceProof {
	public abstract boolean validate(byte[] authenticator, byte[] key);
	public abstract boolean affirmative();
	public abstract byte[] getValue();
	public abstract byte[] getSnapshotId();
}
