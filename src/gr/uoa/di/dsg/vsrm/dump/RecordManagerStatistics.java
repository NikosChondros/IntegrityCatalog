package gr.uoa.di.dsg.vsrm.dump;

import gr.uoa.di.dsg.vsrm.Header;

public class RecordManagerStatistics {
	public class IndexStatistics {
		public int count;
		public long size;
		public int numRecords;
	}
	public class DataBlockStatistics {
		public long count;
		public long size;
		public int recordCount;
		public long recordSize;
		public int recordMinSize;
		public int recordSecondMinSize;
		public int recordMaxSize;
		public int recordAverageSize;
		public int freeCountAboveThreshold;
		public long freeSizeAboveThreshold;
		public int freeCountBelowThreshold;
		public long freeSizeBelowThreshold;
	}
	
    public int headerSize;
    public int blockSize;
    public int freeListThreshold;
    public int ENTRY_EXTRA;
    public int PAGE_EXTRA;
    
	public IndexStatistics indexStats = new IndexStatistics();
	public DataBlockStatistics dataBlockStats = new DataBlockStatistics();
	public Header header;

    public void print() {
        printHeader();
        System.out.format("Index: blocks=%d size=%d recordCount=%d%n", indexStats.count, indexStats.size, indexStats.numRecords);
        System.out.format("Data: blocks=%d size=%d recordCount=%d recordSize=%d recordMinSize=%d recordSecondMinSize=%d recordMaxSize=%d recordAverageSize=%d " +
        		"freeCountAboveThreshold=%d freeSizeAboveThreshold=%d freeCountBelowThreashold=%d freeSizeBelowThreshold=%d totalFreeSize=%d%n", 
        		dataBlockStats.count, dataBlockStats.size, dataBlockStats.recordCount, dataBlockStats.recordSize,
        		dataBlockStats.recordMinSize, dataBlockStats.recordSecondMinSize, dataBlockStats.recordMaxSize, dataBlockStats.recordAverageSize,
        		dataBlockStats.freeCountAboveThreshold, dataBlockStats.freeSizeAboveThreshold,
        		dataBlockStats.freeCountBelowThreshold, dataBlockStats.freeSizeBelowThreshold, dataBlockStats.freeSizeAboveThreshold + dataBlockStats.freeSizeBelowThreshold
        		);
    }
    
    public void printHeader() {
        System.out.format("RecordManager: HeaderSize=%d BlockSize=%d FreeListThreshold=%d EntryExtra=%d PageExtra=%d%n", 
                headerSize, blockSize, freeListThreshold, ENTRY_EXTRA, PAGE_EXTRA);
        System.out.println(header.toString());
    }
}
