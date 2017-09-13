package gr.uoa.di.dsg.treap;

import java.util.ArrayList;
import java.util.function.Consumer;

public class SnapshotExtractorDebug implements ISnapshotExtractor {
	private static class MyInstance implements ISnapshotExtractorInstance {
		private long currentSnapshotId;
		private ArrayList<NodeChanges> listNodeChanges = new ArrayList<>();

		public MyInstance(long currentSnapshotId) {
			this.currentSnapshotId = currentSnapshotId;
		}
		
		public void end() {
			for(NodeChanges nc: listNodeChanges) {
				System.out.format("sid=%d isAppend=%b key=%s value=%s%n", currentSnapshotId, nc.isAppend, new String(nc.key), new String(nc.value));
			}
		}
		
		public void process(Node n) {
			NodeChanges nc = n.getNodeChanges(currentSnapshotId);
			if (nc != null) {
				listNodeChanges.add(nc);
			}
		}
		
		@Override
		public void finishOutput() {
		}
	}
	
	/* (non-Javadoc)
	 * @see gr.uoa.di.dsg.treap.SnapshotExtractor#begin(long)
	 */
	@Override
	public ISnapshotExtractorInstance begin(long snapshotId) {
		return new MyInstance(snapshotId);
	}
	
}
