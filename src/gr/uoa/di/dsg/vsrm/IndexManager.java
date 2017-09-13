/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.uoa.di.dsg.vsrm;

import gr.uoa.di.dsg.vsrm.dump.RecordManagerStatistics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 *
 * @author nikos
 */
public class IndexManager {
    RecordManager host;
    private final ArrayList<Integer> cachedList = new ArrayList<>();
    private final int recsPerPage;
    
    public IndexManager(RecordManager host) throws IOException {
        this.host = host;
        recsPerPage = host.blockSize / (Integer.SIZE / 8) - 1;
        //always populate the index cache from the beginning
        int next = 0, i=0;
        do {
            IndexBlock block = new IndexBlock(host.readBlock(next));
            cachedList.add(next);
            int current = next;
            next = block.getNextIndexBlock();
            if( next > 0 && next <= current )
                throw new RuntimeException(String.format("Index linked listed corrupt: Next of %d is %d", current, next));
            host.release(block);
            i++;
        } while( next > 0);        
    }
    
    public void put(long recNo, int blockNo) throws IOException {
        int indexInCache = (int) (recNo / recsPerPage);
        int positionInBlock = (int) (recNo % recsPerPage);

        //unfortunately, we need to add blank index pages to get to the right id
        while(indexInCache >= cachedList.size()) {
            int lastBlockNo = cachedList.get(cachedList.size() - 1);
            IndexBlock block = new IndexBlock(host.newBlock());
            int newBlockNo = block.getBlockNo();
            cachedList.add(block.getBlockNo());
            host.writeBlock(block);
            host.release(block);
            //and add it to the linked list
            block = new IndexBlock(host.readBlock(lastBlockNo));
            block.setNextIndexBlock(newBlockNo);
            host.writeBlock(block);
            host.release(block);
            
            host.header.incrNumIndexBlocks();
        }
        
        int indexBlockNo = cachedList.get(indexInCache);
        IndexBlock block = new IndexBlock(host.readBlock(indexBlockNo));
        block.put(positionInBlock, blockNo);
        host.writeBlock(block);
        host.release(block);
    }
    
    public int get(long recNo) throws IOException {
        int ret;
        int indexInCache = (int) (recNo / recsPerPage);
        int positionInBlock = (int) (recNo % recsPerPage);
        
        if( indexInCache >= cachedList.size() )
            ret = 0;
        else {
            int indexBlockNo = cachedList.get(indexInCache);
            IndexBlock block = new IndexBlock(host.readBlock(indexBlockNo));
            ret = block.get(positionInBlock);
            host.release(block);
        }
        return ret;
    }
    
    public long getLastRecNo() throws IOException {
        long ret = -1;
        int i = cachedList.size() - 1;
        while( i >= 0 && ret == -1) {
            int indexBlockNo = cachedList.get(i);
            IndexBlock block = new IndexBlock(host.readBlock(indexBlockNo));
            ret = block.getLastRecNo(getIndexBlockFirstRecNo(i));
            host.release(block);
            i--;
        }
        return ret;
    }
    
    /**
     * 
     * @param indexInList the zero-based index of this index page in the list of index pages
     * @return The first record number indexed in this page 
     */
    private long getIndexBlockFirstRecNo(int indexInList) {
        return indexInList * recsPerPage;
    }
    
    protected TreeMap<Long, Integer> dumpAll() throws IOException {
        int i = 0;
        TreeMap<Long, Integer> ret = new TreeMap<>();
        while( i < cachedList.size() ) {
            int indexBlockNo = cachedList.get(i);
            IndexBlock block = new IndexBlock(host.readBlock(indexBlockNo));
            block.dumpAll(ret, getIndexBlockFirstRecNo(i));
            host.release(block);
            i++;
        }
        return ret;
    }
    
    protected void dumpStatistics(RecordManagerStatistics stats) throws IOException {
        int i = 0;
        while( i < cachedList.size() ) {
        	stats.indexStats.count += 1;
            int indexBlockNo = cachedList.get(i);
            IndexBlock block = new IndexBlock(host.readBlock(indexBlockNo));
            block.dumpStatistics(stats);
            host.release(block);
            i++;
        }
        stats.indexStats.size = stats.indexStats.count * stats.blockSize;
    }
    
    protected boolean isIndexBlock(int blockNo) {
        return cachedList.contains(blockNo);
    }
}
