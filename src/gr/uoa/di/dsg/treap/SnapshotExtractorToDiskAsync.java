package gr.uoa.di.dsg.treap;

import gr.uoa.di.dsg.treap.SnapshotExtractorFunctions.SnapshotExtractorState;

import java.util.concurrent.ArrayBlockingQueue;

public class SnapshotExtractorToDiskAsync implements ISnapshotExtractor {
	protected String baseFileName;
	
	public SnapshotExtractorToDiskAsync(String baseFileName) {
		this.baseFileName = baseFileName;
	}

	private class MyInstance implements ISnapshotExtractorInstance {
		private long currentSnapshotId;
		private ArrayBlockingQueue<NodeChanges> queue = new ArrayBlockingQueue<NodeChanges>(256*1024);;
		private Thread consumer;
		private String filename;
		
		public MyInstance(long currentSnapshotId) {
			this.currentSnapshotId = currentSnapshotId;
			filename = SnapshotExtractorFunctions.fileNameOfExtract(baseFileName, currentSnapshotId);
			consumer = new Thread(() -> {consume();});
			consumer.start();
		}
		
		@Override
		public void end() {
			try {
				queue.put(new NodeChanges(true,null,null));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		@Override
		public void process(Node n) {
			NodeChanges nc = n.getNodeChanges(currentSnapshotId);
			if (nc != null) {
				try {
					queue.put(nc);
				} catch (InterruptedException e) {
					e.printStackTrace();
					//TODO: and, now what? This may very well happen in a call through the lambda, very deep in the recursion... 
				}
			}
		}
		
		@Override
		public void finishOutput() {
			try {
				consumer.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		/**
		 * Runs at a secondary thread and consumes NodeChanges objects, outputting them to disk
		 */
		private void consume() {
			try {
				SnapshotExtractorState state = new SnapshotExtractorState(filename);

				while(true) {
					NodeChanges nc = queue.take();
					if( nc.isAppend == true && nc.key == null && nc.value == null ) {
						//this is the end signal
						state.discard();
						return;
					} else {
						SnapshotExtractorFunctions.writeNodeChangesToFile(nc, state);
					}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
				System.exit(1);
			}
		}
		
	}
	
	public ISnapshotExtractorInstance begin(long snapshotId) {
		return new MyInstance(snapshotId);
	}
}
