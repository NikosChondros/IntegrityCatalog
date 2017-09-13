/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.uoa.di.dsg.vsrm;

import gr.uoa.di.dsg.FileManager.FileBlock;
import gr.uoa.di.dsg.util.MemoryBuffer;
import gr.uoa.di.dsg.vsrm.dump.DataBlockEntryDump;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

/**
 *
 * @author nikos
 */
public class DataBlock extends GenericBlock {
    private static final int NUM_ENTRIES_SIZE = Integer.SIZE / Byte.SIZE;
    private static final int NEXT_FREE_BLOCK_SIZE = Integer.SIZE / Byte.SIZE;
    protected static final int TRAILER_SIZE = NUM_ENTRIES_SIZE + NEXT_FREE_BLOCK_SIZE;

    private static final int ENTRY_REC_NO_SIZE = Long.SIZE / Byte.SIZE;
    private static final int ENTRY_POSITION_SIZE = Integer.SIZE / Byte.SIZE;
    private static final int ENTRY_LENGTH_SIZE = Integer.SIZE / Byte.SIZE;

    protected static final int ENTRY_SIZE = ENTRY_REC_NO_SIZE + ENTRY_POSITION_SIZE + ENTRY_LENGTH_SIZE;
    
    private static final int NUM_ENTRIES_REVERSE_OFFSET = NUM_ENTRIES_SIZE;
    private static final int NEXT_FREE_BLOCK_REVERSE_OFFSET = NUM_ENTRIES_REVERSE_OFFSET + NEXT_FREE_BLOCK_SIZE;
    
    private static final int FIRST_ENTRY_REVERSE_OFFSET = ENTRY_SIZE + TRAILER_SIZE;

    private static final int ENTRY_REC_NO_OFFSET = 0;
    private static final int ENTRY_POSITION_OFFSET = ENTRY_REC_NO_OFFSET + ENTRY_REC_NO_SIZE;
    private static final int ENTRY_LENGTH_OFFSET = ENTRY_POSITION_OFFSET + ENTRY_POSITION_SIZE;
    
    DataBlock(FileBlock block) {
        super(block);
    }
    
    private ArrayList<DataBlockEntry> populateEntries() {
        int numEntries = getNumEntries();
        ArrayList<DataBlockEntry> entries = new ArrayList<>(numEntries);
        ByteBuffer buffer = getBuffer();
        int blockSize = buffer.capacity();
        for(int i=0; i<numEntries;i++) {
            int start = blockSize - FIRST_ENTRY_REVERSE_OFFSET - i * ENTRY_SIZE;
            entries.add(new DataBlockEntry(
                    buffer.getLong(start + ENTRY_REC_NO_OFFSET), 
                    buffer.getInt(start + ENTRY_POSITION_OFFSET), 
                    buffer.getInt(start + ENTRY_LENGTH_OFFSET), 
                    i
            ));
        }
        Collections.sort(entries);
        return entries;
    }
    
    private void persistEntries(ArrayList<DataBlockEntry> entries) {
        ByteBuffer buffer = getBuffer();
        int blockSize = buffer.capacity();
        setNumEntries(entries.size());
        int i = 0;
        for( DataBlockEntry entry : entries ) {
            int start = blockSize - FIRST_ENTRY_REVERSE_OFFSET - i * ENTRY_SIZE;
            buffer.putLong(start + ENTRY_REC_NO_OFFSET, entry.recNo);
            buffer.putInt(start + ENTRY_POSITION_OFFSET, entry.position);
            buffer.putInt(start + ENTRY_LENGTH_OFFSET, entry.length);
            entry.realIndex = i;
            i++;
        }
    }
    
    public int getNumEntries() {
        return getBuffer().getInt(getBuffer().capacity() - NUM_ENTRIES_REVERSE_OFFSET);
    }
    
    private void setNumEntries(int value) {
        getBuffer().putInt(getBuffer().capacity() - NUM_ENTRIES_REVERSE_OFFSET, value);
    }
    
    /**
     * 
     * @return The gross free space remaining in this block. Gross because it includes entry overhead (position/length) as well 
     */
    public int getGrossFreeSpace() {
        final int[] used = new int[1];
        used[0] = TRAILER_SIZE;
        walkEntries(new IDataBlockEntryAction() {
            @Override
            public boolean process(int index, long recNo, int position, int length) {
                used[0] += length + ENTRY_SIZE;
                return true;
            }
        });
        return getBuffer().capacity() - used[0];
    }
    
    public int getCurrentNetCapacity() {
        return getBuffer().capacity() - TRAILER_SIZE - getNumEntries() * ENTRY_SIZE;
    }
    
    private void compactRecord(ArrayList<DataBlockEntry> entries) {
        int expectedBeginning = 0;
        ByteBuffer buffer = getBuffer();
        for( DataBlockEntry entry: entries ) {
            int currentOffset = entry.position - expectedBeginning;
            if( currentOffset > 0 ) {
                buffer.position(entry.position);
                buffer.limit(entry.position + entry.length);
                ByteBuffer sourceSclice = buffer.slice(); //source slice with entry
                buffer.clear();
                buffer.position(expectedBeginning);
                buffer.limit(expectedBeginning + entry.length);
                ByteBuffer targetSlice = buffer.slice(); //target slice with proper position for entry
                buffer.clear();
                targetSlice.put(sourceSclice); //move it
                entry.position = expectedBeginning; //and fix the cache
            }
            expectedBeginning += entry.length;
        }
        persistEntries(entries);
    }
    
    public void addRecord(long recNo, MemoryBuffer data) {
        /*
        Alternate implementation:
            Maintain the list sorted by position in the file
        Add:
            for i = 1 to 2
                Iterate through the list from the end going back.
                If space is found, 
                    add it as selected position
                    move all metadata one place off //in the common case, this will not happen
                    update entry's metadata
                    exit loop
                else
                    //no big enough gap was found
                    compact record
                    //keep looping
        */
        /*
        2nd alternate:
            Maintain the list soirted by recNo but keep separate parallel linked list with position sort
            Allows binary search for recNo (fast lookups), while accessing the list allows for fast add
        */
        //*DEBUG*/if( getBlockNo() == 40 )
            //*DEBUG*/System.out.format("add %d l=%d%n", recNo, data.length);
        ArrayList<DataBlockEntry> entries = populateEntries();
        int dataSize = data.size();
        int expectedEntryMetadataPosition = getBuffer().capacity() - FIRST_ENTRY_REVERSE_OFFSET; 
        //if the last entry in this block overlaps with the space for this entry's metadata, we need to compact this record
        if( ! entries.isEmpty() ) {
//            DataBlockEntry entry = entries.last();
            DataBlockEntry entry = entries.get(entries.size()-1);
            expectedEntryMetadataPosition = getCurrentNetCapacity() - ENTRY_SIZE;
            if( entry.position + entry.length > expectedEntryMetadataPosition )
                compactRecord(entries);
        }
        
        DataBlockEntry selectedGap = null;
        int passes = 0; //passess protects against an infinite loop if this method is called to place a record bigger than its available space
        do {
            ArrayList<DataBlockEntry> gaps = new ArrayList<>();
            //identify all gaps. Algorithm below assumes entries are sorted by position
            int expectedBeginning = 0;
            for( DataBlockEntry entry: entries ) {
                if( entry.position > expectedBeginning ) {
                    gaps.add(new DataBlockEntry(0, expectedBeginning, entry.position - expectedBeginning, 0));
                }
                expectedBeginning = entry.position + entry.length;
            }
            // and add the last region
            int remaining = expectedEntryMetadataPosition - expectedBeginning;
            //if( remaining > 0) allow this as zero length entries can use this space as long as ENTRY_SIZE metadata space is available
            gaps.add(new DataBlockEntry(0, expectedBeginning, remaining, 0));
            Collections.sort(gaps);

            //is there a big enough gap for data.length bytes?
            for( DataBlockEntry gap: gaps ) {
                if( gap.length >= dataSize && gap.position + dataSize <= expectedEntryMetadataPosition ) {
                    selectedGap = gap;
                    passes--;
                    break;
                }
            }
            if( selectedGap == null )
                compactRecord(entries); // and try again
            passes++;
        } while( selectedGap == null && passes <= 2 );
        if( passes > 2 ) {
            throw new RuntimeException("DataBlock.addRecord() failed to add block twice. What happened here?");
        }
        entries.add(new DataBlockEntry(recNo, selectedGap.position, dataSize, -1));
        persistEntries(entries);
        getBuffer().position(selectedGap.position);
        //getBuffer().put(data);
        data.write(getBuffer());
    }
    
    public int delRecord(final long  recNo) {
        final int ret[] = new int[1];
        final ByteBuffer buffer = getBuffer();
        final int blockSize = buffer.capacity();
        WalkerReturn wret = walkEntries(new IDataBlockEntryAction() {
            @Override
            public boolean process(int index, long precNo, int position, int length) {
                if( precNo == recNo ) {
                    ret[0] = length;
                    int numEntries = getNumEntries();
                    setNumEntries( numEntries - 1); //this is safe here regarding the walker
                    //and now move everything beyond this point entry by entry (respecting any sorting order)
                    int first = blockSize - FIRST_ENTRY_REVERSE_OFFSET;
                    for( int i = index + 1; i < numEntries; i++ ) {
                        int sourcePosition = first - i * ENTRY_SIZE;
                        int targetPosition = first - (i - 1) * ENTRY_SIZE;
                        for( int ic = 0; ic < ENTRY_SIZE; ic++)
                            buffer.put(targetPosition + ic, buffer.get(sourcePosition + ic));
                    }
                    //and update the cache
                    return false;
                }
                return true;
            }
        });
        
        //*DEBUG*/if( getBlockNo() == 40 )
            //*DEBUG*/System.out.format("del %d -> %d%n", recNo, ret[0]);
        if( wret == WalkerReturn.STOPPED )
            return ret[0];
        else
            throw new IllegalArgumentException(String.format("DataBlock.delete(%d): record not found in page. Index was wrong?", recNo));
        
    }
    
    public byte[] getRecord(final long recNo) {
        final byte[][] ret = new byte[1][];
        
        WalkerReturn wret = walkEntries(new IDataBlockEntryAction() {
            @Override
            public boolean process(int index, long precNo, int position, int length) {
                if( precNo == recNo ) {
                    ret[0] = getData(position, length);
                    return false;
                }
                return true;
            }
        });
        
        if( wret == WalkerReturn.STOPPED )
            return ret[0];
        else
            throw new IllegalArgumentException(String.format("DataBlock.getRecord(%d): Record not found in page. Index was wrong?", recNo));
    }
    
    private byte[] getData(int position, int length) {
        byte[] ret = new byte[length];
        getBuffer().position(position);
        getBuffer().get(ret);
        return ret;
    }
    
    public int getRecordLength(final long recNo) {
        final int[] ret = new int[1];
        WalkerReturn wret = walkEntries(new IDataBlockEntryAction() {
            @Override
            public boolean process(int index, long precNo, int position, int length) {
                if (recNo == precNo ) {
                    ret[0] = length;
                    return false;
                }
                return true;
            }
        });
        if( wret == WalkerReturn.STOPPED )
            return ret[0];
        else
            throw new RuntimeException(String.format("DataBlock.getRecordLength(%d): Record not found in page. Index was wroing?", recNo));
    }
    
    public int getNextFreeBlock() {
        return getBuffer().getInt(getBuffer().capacity() - NEXT_FREE_BLOCK_REVERSE_OFFSET);
    }
    
    public void setNextFreeBlock(int no) {
        getBuffer().putInt(getBuffer().capacity() - NEXT_FREE_BLOCK_REVERSE_OFFSET, no);
    }
 
     private enum WalkerReturn {
        EMPTY, STOPPED, EXHAUSTED
    }
    
    /**
     * 
     * @param action
     * @return 0 when list is empty, 1 when action instructed walker to stop, 2 when list was exhausted without action signaling to stop
     * @throws IOException 
     */
    private WalkerReturn walkEntries(IDataBlockEntryAction action) {
        int numEntries = getNumEntries();
        if( numEntries == 0 )
            return WalkerReturn.EMPTY;
        
        ByteBuffer buffer = getBuffer();
        int first = buffer.capacity() - FIRST_ENTRY_REVERSE_OFFSET;
        for(int i = 0; i < numEntries; i++) {
            int start = first - i * ENTRY_SIZE;
            if (! action.process(i,
                    buffer.getLong(start + ENTRY_REC_NO_OFFSET),
                    buffer.getInt(start + ENTRY_POSITION_OFFSET), 
                    buffer.getInt(start + ENTRY_LENGTH_OFFSET)))
                return WalkerReturn.STOPPED;
        }
        return WalkerReturn.EXHAUSTED;
    }
    
    protected ArrayList<DataBlockEntryDump> dumpAll() {
        final ArrayList<DataBlockEntryDump> ret = new ArrayList<>();
        walkEntries(new IDataBlockEntryAction() {
            @Override
            public boolean process(int index, long recNo, int position, int length) {
                DataBlockEntryDump d = new DataBlockEntryDump();
                d.recNo = recNo;
                d.position = position;
                d.length = length;
                d.data = getData(position, length);
                ret.add(d);
                return true;
            }
        });
        return ret;
    }

    protected int[] dumpStats() {
    	//0:count, 1:tot_size, 2=min_size, 3=max_size
    	final int metrics[] = new int[4];
    	metrics[2] = Integer.MAX_VALUE;
        walkEntries(new IDataBlockEntryAction() {
            @Override
            public boolean process(int index, long recNo, int position, int length) {
                metrics[0] += 1;
                metrics[1] += length;
                if( length < metrics[2] )
                	metrics[2] = length;
                if( length > metrics[3] )
                	metrics[3] = length;
                return true;
            }
        });
        return metrics;
    }
}
 