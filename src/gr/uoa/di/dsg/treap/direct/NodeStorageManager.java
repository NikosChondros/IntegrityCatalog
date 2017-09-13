package gr.uoa.di.dsg.treap.direct;

import java.util.ArrayList;

public class NodeStorageManager {
	ArrayList<Node> nodes = new ArrayList<>();
	long root = Tree.TERMINAL_NODE_ID;
			
	long nextId() {
		return nodes.size();
	}
	
	void update(Node node) {
		int id = (int) node.getId();
		while( nodes.size() <= id )
			nodes.add(null);
		nodes.set(id, node);
	}
	
	Node get(long lid) {
		if( lid == Tree.TERMINAL_NODE_ID )
			return null;
		else
			return nodes.get((int) lid);
	}
	
	long getRoot() {
		return root;
	}
	
	void setRoot(long newRoot) {
		root = newRoot;
	}
}
