package gr.uoa.di.dsg.treap;

import java.io.IOException;
import java.util.ArrayList;

public class TreePath {
	private ArrayList<TreeEdge> edges = new ArrayList<>();
	
	public TreePath(TreeEdge edge) {
		edges.add(edge);
	}
	
	public TreeEdge last() {
		return edges.get(edges.size() - 1);
	}
	/*
	public TreeEdge beforeLast() {
		return edges.get(edges.size() - 2);
	}
	*/
	public void add(TreeEdge edge) {
		edges.add(edge);
	}
	
	public void removeLast() {
		edges.remove(edges.size() - 1);
	}
	
	public void replaceEdgeTarget(long oldId, long newId) throws IOException {
		for (TreeEdge edge: edges) {
			if( edge.follow() == oldId ) {
				edge.replace(newId, this);
				break;
			}
		}
	}
	
	public void replaceEdgeSource(Node oldNode, Node newNode) {
		for( int i = edges.size() - 1; i >= 0; i--) {
			TreeEdge oldEdge = edges.get(i);
			TreeEdge newEdge;
			if ( oldEdge.host() == oldNode ) {
				if (oldEdge.isLeft() )
					newEdge = newNode.getLeftEdge();
				else
					newEdge = newNode.getRightEdge();
				edges.set(i, newEdge);
				break;
			}
		}
	}
	
	public int depth() {
		//depth is zero based, need to remove "outside" edge to root
		return edges.size() - 1;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		String sep = "";
		for (TreeEdge edge : edges) {
			sb.append(sep);
			Node host = edge.host(); 
			if (host != null)
				sb.append(host.getId());
			sb.append("->");
			sb.append(edge.follow());
			sep=",";
		}
		return sb.toString();
	}
}
