package gr.uoa.di.dsg.treap;

public interface ISnapshotExtractor {

	public abstract ISnapshotExtractorInstance begin(long snapshotId);

}