/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.uoa.di.dsg.FileManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.PriorityQueue;

/**
 *
 * @author nikos
 */
public class FileManager implements IFileManager {
    private static final int CACHE_SIZE = 100;
    
    private class UsedEntry {
        public int refCount;
        public FileBlock block;

        public UsedEntry(FileBlock block) {
            this.block = block;
            this.refCount = 1;
        }
        
        public void addRef() {
            refCount++;
        }
        
        public boolean delRef() {
            refCount--;
            return refCount == 0;
        }
    }
    //statistics
    private int blocksReused = 0;
    private int blocksInCache = 0;
    private int blocksFromCache = 0;
    private int blocksAllocated = 0;
    
    private class CacheEntry implements Comparable<CacheEntry> {
        long accessTime;
        FileBlock block;

        public CacheEntry(FileBlock block) {
            this.accessTime = System.currentTimeMillis();
            this.block = block;
        }
        
        @Override
        public int compareTo(CacheEntry t) {
            return Long.compare(accessTime, t.accessTime);
        }
    }
    private FileChannel fc;
    private final int headerSize;
    private final int blockSize;
    
    private final ByteBuffer headerBuffer;
    private final HashMap<Integer, UsedEntry> blocksInUse = new HashMap<>();
    private final HashMap<Integer, CacheEntry> freeBlocksByBlockNo = new HashMap<>();
    private final PriorityQueue<CacheEntry> freeBlocksByAccessTime = new PriorityQueue<>();

    private final byte[] emptyBlock; 
    private final boolean useDirect;

    public FileManager(String fileName,int headerSize, int blockSize, boolean useDirect) throws FileNotFoundException {
        this.headerSize = headerSize;
        this.blockSize = blockSize;
        this.useDirect = useDirect;
        
        RandomAccessFile aFile;
        aFile = new RandomAccessFile(fileName, "rw");
        fc = aFile.getChannel();
        
        emptyBlock = new byte[blockSize];
        if( useDirect )
            headerBuffer = ByteBuffer.allocateDirect(headerSize);
        else
            headerBuffer = ByteBuffer.allocate(headerSize);
    }

    @Override
    public int getBlockSize() {
        return blockSize;
    }

    @Override
    public int getHeaderSize() {
        return headerSize;
    }
    
    @Override
    public ByteBuffer getHeaderBuffer() {
        return headerBuffer;
    }
    
    @Override
    public void readHeader() throws IOException {
        fc.position(0);
        headerBuffer.rewind();
        fc.read(headerBuffer);
    }
    
    @Override
    public void writeHeader() throws IOException {
        fc.position(0);
        headerBuffer.rewind();
        fc.write(headerBuffer);
    }

    @Override
    public FileBlock emptyBlock(int blockNo) {
        ByteBuffer buffer = getByteBuffer();
        buffer.put(emptyBlock);
        FileBlock block = new FileBlock(blockNo, buffer);
        blocksInUse.put(blockNo, new UsedEntry(block));
        return block;
    }
    
    @Override
    public FileBlock readBlock(int blockNo) throws IOException {
        //is the block currently used?
        UsedEntry used = blocksInUse.get(blockNo);
        if( used != null ) {//if yes, return the existing copy
            blocksReused++;
            used.addRef();
            return used.block;
        }
        
        //is the block in the cache right now?
        CacheEntry cached = freeBlocksByBlockNo.remove(blockNo);
        if( cached != null ) {
            blocksInCache++;
            freeBlocksByAccessTime.remove(cached);
            blocksInUse.put(cached.block.getBlockNo(), new UsedEntry(cached.block));
            return cached.block;
        }
        
        ByteBuffer buffer = getByteBuffer();
        
        fc.position((long) headerSize + (long) blockNo * (long) blockSize);
        fc.read(buffer);
        
        FileBlock block = new FileBlock(blockNo, buffer);
        blocksInUse.put(blockNo, new UsedEntry(block));
        return block;
    }
 
    private ByteBuffer getByteBuffer() {
        ByteBuffer buffer;
        
        //get the oldest from the cache
        CacheEntry entry = null;
        if( blocksInUse.size() + freeBlocksByBlockNo.size() < CACHE_SIZE )
            entry = null; //let it allocate a few more buffers when our cache is not yet full
        else
            entry = freeBlocksByAccessTime.poll();
        if( entry != null ) {
            blocksFromCache++;
            freeBlocksByBlockNo.remove(entry.block.getBlockNo());
            buffer = entry.block.getBuffer();
            buffer.rewind();
        } else {
            blocksAllocated++;
            if( useDirect )
                buffer = ByteBuffer.allocateDirect(blockSize);
            else
                buffer = ByteBuffer.allocate(blockSize);
        }
        return buffer;
    }
    
    @Override
    public void releaseBlock(FileBlock block) {
        UsedEntry used = blocksInUse.get(block.getBlockNo());
        if( used.delRef() ) {
            if( freeBlocksByBlockNo.size() >= CACHE_SIZE ) {
                //evict the earliest
                CacheEntry entry = freeBlocksByAccessTime.poll();
                freeBlocksByBlockNo.remove(entry.block.getBlockNo());
            }
            CacheEntry entry = new CacheEntry(block);
            freeBlocksByAccessTime.add(entry);
            freeBlocksByBlockNo.put(block.getBlockNo(), entry);
            blocksInUse.remove(block.getBlockNo());
        }
    }
    
    @Override
    public void writeBlock(FileBlock block) throws IOException {
        fc.position((long) headerSize + (long) block.getBlockNo() * (long) blockSize);
        ByteBuffer buffer = block.getBuffer();
        buffer.rewind();
        fc.write(buffer);
    }
    
    @Override
    public void close() throws IOException {
        fc.close();
        fc = null;
    }
    
    @Override
    public boolean isEmpty() throws IOException {
        return fc.size() == 0;
    }
    
//    private String batchName;
    @Override
    public void beginBatch(String name) {
//        batchName = name;
    }
    
    @Override
    public void endBatch() {
//        String prefix = "";
//        StringBuilder sb = new StringBuilder();
//        for( int i: blocksInUse.keySet() ) {
//            sb.append(prefix);
//            sb.append(i);
//            prefix=",";
//        }
//        System.out.format("end batch %s: inUse=%d(%s), cached=%d%n", batchName, blocksInUse.size(), sb.toString(), freeBlocksByBlockNo.size());
    }
    
    @Override
    public long getFileSize() throws IOException {
    	return fc.size();
    }
    
    @Override
    public void reportStatistics() {
        System.out.format("FileManager stats: reused=%d in cache=%d from cache=%d allocated=%d%n", blocksReused, blocksInCache, blocksFromCache, blocksAllocated);
    }
}
