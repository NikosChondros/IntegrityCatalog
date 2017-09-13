package gr.uoa.di.dsg.treap;

public interface ISnapshotExtractorInstance {
	void process(Node n);
	void end();
	void finishOutput();
}
