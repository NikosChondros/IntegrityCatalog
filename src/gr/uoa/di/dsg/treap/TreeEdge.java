package gr.uoa.di.dsg.treap;

import java.io.IOException;

public interface TreeEdge {
	public long follow();
	public void replace(long other, TreePath path) throws IOException;
	public Node host();
	public boolean isLeft();
}
