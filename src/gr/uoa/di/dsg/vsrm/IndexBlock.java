/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.uoa.di.dsg.vsrm;

import gr.uoa.di.dsg.FileManager.FileBlock;
import gr.uoa.di.dsg.vsrm.dump.RecordManagerStatistics;

import java.nio.IntBuffer;
import java.util.TreeMap;

/**
 *
 * @author nikos
 */
public class IndexBlock extends GenericBlock {
    private static final int INTEGER_BYTE_SIZE = Integer.SIZE / Byte.SIZE;
    private static final int NEXT_INDEX_BLOCK_SIZE = INTEGER_BYTE_SIZE;
    private static final int NEXT_INDEX_BLOCK_REVERSE_OFFSET = NEXT_INDEX_BLOCK_SIZE;
    
    public IndexBlock(FileBlock block) {
        super(block);
    }
    
    public int getNextIndexBlock() {
        return getBuffer().getInt(getBuffer().capacity() - NEXT_INDEX_BLOCK_REVERSE_OFFSET);
    }
    
    public void setNextIndexBlock(int value) {
        getBuffer().putInt(getBuffer().capacity() - NEXT_INDEX_BLOCK_REVERSE_OFFSET, value);
    }
    
    public long getLastRecNo(long firstRecNoInBlock) {
        getBuffer().rewind();
        IntBuffer ib = getBuffer().asIntBuffer();
        int i = ib.limit() - 2;
        while( i >= 0 ) {
            int ptr = ib.get(i);
            if( ptr > 0 )
                return firstRecNoInBlock + i;
            i--;
        }
        return -1;
    }
    
    public void put(int indexInBlock, int blockNo) {
        getBuffer().putInt(indexInBlock * INTEGER_BYTE_SIZE, blockNo);
    }
    
    public int get(int indexInBlock) {
        return getBuffer().getInt(indexInBlock * INTEGER_BYTE_SIZE);
    }
    
    protected void dumpAll(TreeMap<Long, Integer> dump, long firstRecNo) {
        getBuffer().rewind();
        IntBuffer ib = getBuffer().asIntBuffer();
        for(int i = 0; i < ib.limit() - 1; i++) {
            int v = ib.get(i);
            if( v > 0 )
                dump.put(firstRecNo + i, v);
        }
    }

    protected void dumpStatistics(RecordManagerStatistics stats) {
        getBuffer().rewind();
        IntBuffer ib = getBuffer().asIntBuffer();
        for(int i = 0; i < ib.limit() - 1; i++) {
            int v = ib.get(i);
            if( v > 0 )
                stats.indexStats.numRecords += 1;
        }
    }
}
