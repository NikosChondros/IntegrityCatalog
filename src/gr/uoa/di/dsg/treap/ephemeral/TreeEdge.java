package gr.uoa.di.dsg.treap.ephemeral;

public interface TreeEdge {
	public long follow();
	public void replace(long other);
}
