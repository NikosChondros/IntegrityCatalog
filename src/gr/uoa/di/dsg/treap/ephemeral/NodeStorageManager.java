package gr.uoa.di.dsg.treap.ephemeral;

import java.util.ArrayList;

public class NodeStorageManager {
	ArrayList<byte[]> nodes = new ArrayList<>();
	long root = Tree.TERMINAL_NODE_ID;
			
	long nextId() {
		return nodes.size();
	}
	
	void update(Node node) {
		long id = node.getId();
		while( nodes.size() <= id )
			nodes.add(null);
		nodes.set((int) id, node.getBuffer());
	}
	
	Node get(long lid) {
		return new Node(lid, nodes.get((int) lid));
	}
	
	long getRoot() {
		return root;
	}
	
	void setRoot(long newRoot) {
		root = newRoot;
	}
}
