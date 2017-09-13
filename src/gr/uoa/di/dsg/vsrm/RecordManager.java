/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.uoa.di.dsg.vsrm;

import gr.uoa.di.dsg.FileManager.FileBlock;
import gr.uoa.di.dsg.FileManager.FileManager;
import gr.uoa.di.dsg.FileManager.IFileManager;
import gr.uoa.di.dsg.util.MemoryBuffer;
import gr.uoa.di.dsg.vsrm.dump.DataBlockDump;
import gr.uoa.di.dsg.vsrm.dump.RecordManagerDump;
import gr.uoa.di.dsg.vsrm.dump.RecordManagerStatistics;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 *
 * @author nikos
 */
public class RecordManager implements Closeable {
    private static final int ENTRY_EXTRA = DataBlock.ENTRY_SIZE;
    
    protected final int blockSize;
    protected final int headerSize;
    protected final int freeListThreshold;  //any block with free space >= of this participates in the free list 

    private IFileManager file;
    protected Header header;
    private FreeListManager freeList;
    private IndexManager index;
    
    private DataBlock lastBlock;
    
    public RecordManager(IFileManager file, int freeListThreshold) throws IOException, IllegalArgumentException {
        this.blockSize = file.getBlockSize();
        this.headerSize = file.getHeaderSize();
        this.freeListThreshold = freeListThreshold;
        this.file = file;
        if( headerSize < Header.MIN_SIZE )
            throw new IllegalArgumentException("header size is less than the minimum of " + Header.MIN_SIZE);
        beginBatch("init");
        boolean fileIsEmpty = file.isEmpty();
        header = new Header(file.getHeaderBuffer());
        
        if( fileIsEmpty ) {
            //initialize file
            header.setNumDataBlocks(1); //defined below
            header.setNumIndexBlocks(1);
            header.setLastDataBlock(1);
            header.setNumBlocks(2);
            FileBlock block = file.emptyBlock(0); //first index page
            file.writeBlock(block);
            file.releaseBlock(block);
            
            block = file.emptyBlock(1); //first (and last) data page
            file.writeBlock(block);
            lastBlock = new DataBlock(block);
        } else {
            readHeader();
            lastBlock = new DataBlock(readBlock(header.getLastDataBlock()));
        }
        //these should be initialized after the RecordManager is fully initialized
        freeList = new FreeListManager(this);
        index = new IndexManager(this);
        endBatch();
    }
    
    protected final FileBlock readBlock(int blockNo) throws IOException {
        return file.readBlock(blockNo);
    }
    
    protected FileBlock newBlock() throws IOException {
        return file.emptyBlock(header.incrNumBlocks());
    }
    
    protected void writeBlock(GenericBlock block) throws IOException {
        file.writeBlock(block.getBlock());
    }
    
    private void readHeader() throws IOException {
        file.readHeader();
        header.fromBuffer();
        header.dirty = false;
    }
    
    protected final void writeHeader() throws IOException {
    	if( header.dirty ) {
    		header.toBuffer();
    		file.writeHeader();
    		header.dirty = false;
    	}
    }
    
    protected void release(GenericBlock block) {
        file.releaseBlock(block.getBlock());
    }
    
    public byte[] get(long recNo) throws IOException {
    	beginBatch("get");
        int blockNo = index.get(recNo);
        if( blockNo == 0 )
            return null;
        DataBlock block = new DataBlock(readBlock(blockNo));
        byte[] ret = block.getRecord(recNo);
        release(block);
        endBatch();
        return ret;
    }
    
    public void add(long recNo, byte[] data) throws IOException {
    	add(recNo, new MemoryBuffer(data));
    }
    
    public final int maxRecordLength() {
    	return blockSize - DataBlock.TRAILER_SIZE - DataBlock.ENTRY_SIZE;
    }
    
    public void add(long recNo, MemoryBuffer data) throws IOException {
    	int dataSize = data.size();
        if( dataSize > maxRecordLength() ) 
            throw new IllegalArgumentException(String.format("Data length %d exceeds maximum supported length of %d", dataSize, maxRecordLength()));
        beginBatch("add");
        int neededSpace = dataSize + ENTRY_EXTRA;
        //walk the free list trying to find a record with big enough free space
        
        int blockNo = freeList.findBlock(neededSpace);
        DataBlock block;
        if( blockNo > 0 ) {
            block = new DataBlock(readBlock(blockNo));
            int currentFree = block.getGrossFreeSpace();
            block.addRecord(recNo, data);
            freeList.checkUpdatedBlock(block, currentFree);
            writeBlock(block);
            release(block);
            index.put(recNo, block.getBlockNo());
        } else {
            int freeSpaceInLast = lastBlock.getGrossFreeSpace();
            if ( freeSpaceInLast < neededSpace ) {
                //release the lastBlock
                freeList.addBlock(lastBlock);
                //allocate new free block
                release(lastBlock);
                lastBlock = new DataBlock(newBlock());
                header.incrNumDataBlocks();
                header.setLastDataBlock(lastBlock.getBlockNo());
            } 
            lastBlock.addRecord(recNo, data);
            writeBlock(lastBlock);
            //no need to worry about free list for last page
            index.put(recNo, lastBlock.getBlockNo());
        }
        header.addRecord(dataSize);
        endBatch();
    }
    
    public void update(long recNo, byte[] data) throws IOException {
    	update(recNo, new MemoryBuffer(data));
    }
    
    public void update(long recNo, MemoryBuffer data) throws IOException {
    	int dataSize = data.size();
        if( dataSize > maxRecordLength() ) 
            throw new IllegalArgumentException(String.format("Data length %d exceeds maximum supported length of %d", dataSize, maxRecordLength()));
        beginBatch("update");
        int blockNo = index.get(recNo);
        if( blockNo == 0 )
            throw new IllegalArgumentException(String.format("Item %d not found for edit", recNo));
        DataBlock block = new DataBlock(readBlock(blockNo));
        
        int currentLength = block.getRecordLength(recNo);
        int currentFree = block.getGrossFreeSpace();
        if( currentLength != dataSize )
            header.delRecord(currentLength);
        if ( dataSize <= currentLength || dataSize <= currentFree + currentLength  )  {
            block.delRecord(recNo);
            block.addRecord(recNo, data);
            //but leave the index unchanged
            if( block.getBlockNo() != lastBlock.getBlockNo() )
                freeList.checkUpdatedBlock(block, currentFree);
            writeBlock(block);
            release(block);
            if( currentLength != dataSize )
                header.addRecord(dataSize);
        } else {
            block.delRecord(recNo);
            if( block.getBlockNo() != lastBlock.getBlockNo() )
                freeList.checkUpdatedBlock(block, currentFree);
            writeBlock(block);
            release(block);
            add(recNo, data); //will also modify the index
        }
        //*STATS*/if( currentLength != data.length )
            //*STATS*/writeHeader();
        endBatch();
    }
    
    public void delete(long recNo) throws IOException {
    	beginBatch("delete");
        int blockNo = index.get(recNo);
        DataBlock block = new DataBlock(readBlock(blockNo));
        
        int currentFree = block.getGrossFreeSpace();
        int currentLength = block.delRecord(recNo);
        writeBlock(block);
        if( block.getBlockNo() != lastBlock.getBlockNo() )
            freeList.checkUpdatedBlock(block, currentFree);
        release(block);
        index.put(recNo, 0);
        header.delRecord(currentLength);
        //*STATS*/writeHeader();
        endBatch();
    }
    
    public long getLastRecNo() throws IOException {
    	beginBatch("getLast");
        long ret = index.getLastRecNo();
        endBatch();
        return ret;
    }
    
    @Override
    public void close() throws IOException {
        file.close();
    }
    
    private final void beginBatch(String id) {
    	file.beginBatch(id);
    }
    
    private final void endBatch() throws IOException {
    	writeHeader();
    	file.endBatch();
    }
    
    public int getLargestRecordSize() {
    	return header.getMaxRecordLength();
    }
    
    public long getFileSize() throws IOException {
    	return file.getFileSize();
    }
    
    public RecordManagerDump getCompleteDump() throws IOException {
    	beginBatch("dump");
        RecordManagerDump ret = new RecordManagerDump();
        ret.headerSize = headerSize;
        ret.blockSize = blockSize;
        ret.freeListThreshold = freeListThreshold;
        ret.ENTRY_EXTRA = DataBlock.ENTRY_SIZE;
        ret.PAGE_EXTRA = DataBlock.TRAILER_SIZE;
        
        ret.header = header;
        //index entries: map<long, int>
        //data pages: map<int, DataPage>
        //data page: ArrayList<Entry>
        
        //first read all index pages
        ret.indexEntries = index.dumpAll();
        long numBlocks = header.getNumBlocks();
        for(int blockNo=1; blockNo < numBlocks; blockNo++) {
            if( ! index.isIndexBlock(blockNo) ) {
                DataBlock block = new DataBlock(readBlock(blockNo));
                DataBlockDump d = new DataBlockDump();
                d.blockNo = blockNo;
                d.nextFree = block.getNextFreeBlock();
                d.numEntries = block.getNumEntries();
                d.freeSpace = block.getGrossFreeSpace();
                d.entries = block.dumpAll();
                ret.dataBlocks.put(blockNo, d);
                release(block);
            }
        }
        ret.freeList = freeList.dumpList();
        endBatch();
        return ret;
    }
    
    public RecordManagerStatistics getStatisticsDump() throws IOException {
    	beginBatch("dump");
        RecordManagerStatistics ret = new RecordManagerStatistics();
        ret.headerSize = headerSize;
        ret.blockSize = blockSize;
        ret.freeListThreshold = freeListThreshold;
        ret.ENTRY_EXTRA = DataBlock.ENTRY_SIZE;
        ret.PAGE_EXTRA = DataBlock.TRAILER_SIZE;
        
        ret.header = header;
        index.dumpStatistics(ret);
        
        ret.dataBlockStats.recordMinSize = Integer.MAX_VALUE;
        long numBlocks = header.getNumBlocks();
        for(int blockNo=1; blockNo < numBlocks; blockNo++) {
            if( ! index.isIndexBlock(blockNo) ) {
            	ret.dataBlockStats.count += 1;
                DataBlock block = new DataBlock(readBlock(blockNo));
                int freeSpace = block.getGrossFreeSpace();
                if( freeSpace > freeListThreshold ) {
                	ret.dataBlockStats.freeCountAboveThreshold += 1;
                	ret.dataBlockStats.freeSizeAboveThreshold += freeSpace;
                } else {
                	ret.dataBlockStats.freeCountBelowThreshold += 1;
                	ret.dataBlockStats.freeSizeBelowThreshold += freeSpace;
                }
                int[] blockMetrics = block.dumpStats();
                ret.dataBlockStats.recordCount += blockMetrics[0];
                ret.dataBlockStats.recordSize += blockMetrics[1];
                if( blockMetrics[2] < ret.dataBlockStats.recordMinSize ) {
                	ret.dataBlockStats.recordSecondMinSize = ret.dataBlockStats.recordMinSize; 
                	ret.dataBlockStats.recordMinSize = blockMetrics[2]; 
                } else {
                	if (blockMetrics[2] > ret.dataBlockStats.recordMinSize && blockMetrics[2] < ret.dataBlockStats.recordSecondMinSize) {
                		ret.dataBlockStats.recordSecondMinSize = blockMetrics[2]; 
                	}
                }
                if( blockMetrics[3] > ret.dataBlockStats.recordMaxSize )
                	ret.dataBlockStats.recordMaxSize = blockMetrics[3]; 
                release(block);
            }
        }
        
        ret.dataBlockStats.size = (long) ret.dataBlockStats.count * (long) blockSize;
        if( ret.dataBlockStats.recordCount > 0 )
        	ret.dataBlockStats.recordAverageSize = (int) (ret.dataBlockStats.recordSize / ret.dataBlockStats.recordCount);
        else
        	ret.dataBlockStats.recordAverageSize = 0;
        endBatch();
        return ret;
    }
    
    private static final String encoding = "ASCII";
    private static byte[] s2b(String s) throws UnsupportedEncodingException {
        return s.getBytes(encoding);
    }
    private static String b2s(byte[] b) throws UnsupportedEncodingException {
        return new String(b, encoding);
    }
    
    public static void main(String args[]) throws Exception {
        long lastRecNo;
        String s;
        final String filename = "/home/nikos/temp/rm.dat";
        new File(filename).delete();
        FileManager file = new FileManager(filename, 128, 64, true);
//        MappedFileManager file = new MappedFileManager(filename, 128, 64, 8192, MappedFileManager.WritePolicy.ON_WRITE);
        RecordManager rm = new RecordManager(file, 1);
        lastRecNo = rm.getLastRecNo();
        rm.add(0, s2b("Nikos Chondros"));
        lastRecNo = rm.getLastRecNo();
        rm.add(1, s2b("Dina Vassi"));
        lastRecNo = rm.getLastRecNo();
        rm.add(2, s2b("Mema Roussopoulos"));
        lastRecNo = rm.getLastRecNo();
        
        rm.getCompleteDump().printFull(encoding);
        
        s = b2s(rm.get(0));
        s = b2s(rm.get(1));
        s = b2s(rm.get(2));
        
        rm.delete(1);
        lastRecNo = rm.getLastRecNo();
        rm.delete(2);
        lastRecNo = rm.getLastRecNo();
        rm.delete(0);
        lastRecNo = rm.getLastRecNo();
        
        file.reportStatistics();
        rm.close();
    }
}
