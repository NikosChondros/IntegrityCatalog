/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.uoa.di.dsg.vsrm.dump;

import gr.uoa.di.dsg.vsrm.FreeListEntry;
import gr.uoa.di.dsg.vsrm.Header;

import java.util.ArrayList;
import java.util.TreeMap;

/**
 *
 * @author nikos
 */
public class RecordManagerDump {
    public int headerSize;
    public int blockSize;
    public int freeListThreshold;
    public int ENTRY_EXTRA;
    public int PAGE_EXTRA;
    
    public Header header;
    public TreeMap<Long, Integer> indexEntries;
    public TreeMap<Integer, DataBlockDump> dataBlocks = new TreeMap<>();
    public ArrayList<FreeListEntry> freeList;


    public void printFull(String dataEncoding) throws Exception {
        printHeader();
        System.out.print("Index entries:");
        for(long recNo: indexEntries.keySet())
            System.out.format(" %d->%d", recNo, indexEntries.get(recNo));
        System.out.println();
        
        System.out.println("Data pages:");
        for(int blockNo: dataBlocks.keySet()) {
            DataBlockDump bd = dataBlocks.get(blockNo);
            System.out.format("%d: nextFree=%d, free=%d, numEntries=%d. Entries:", 
                    bd.blockNo, bd.nextFree, bd.freeSpace, bd.numEntries);
            for( DataBlockEntryDump entry: bd.entries )
                System.out.format(" r=%d p=%d l=%d '%s'", entry.recNo, entry.position, entry.length, new String(entry.data, dataEncoding));
            System.out.println();
        }
        
        System.out.print("Free list:");
        for( FreeListEntry entry : freeList )
            System.out.format(" %d:%d", entry.blockNo, entry.freeSpace);
        System.out.println();
    }
    
    public void printAlmostFull() throws Exception {
        printHeader();
        System.out.print("Index entries:");
        for(long recNo: indexEntries.keySet())
            System.out.format(" %d->%d", recNo, indexEntries.get(recNo));
        System.out.println();
        
        System.out.println("Data pages:");
        for(int blockNo: dataBlocks.keySet()) {
            DataBlockDump bd = dataBlocks.get(blockNo);
            System.out.format("%d: nextFree=%d, free=%d, numEntries=%d%n", 
                    bd.blockNo, bd.nextFree, bd.freeSpace, bd.numEntries);
        }
        
        System.out.print("Free list:");
        for( FreeListEntry entry : freeList )
            System.out.format(" %d:%d", entry.blockNo, entry.freeSpace);
        System.out.println();
    }
    
    public void printSummary() {
        printHeader();
        System.out.format("Index entries:%d%n", indexEntries.size());
        
        System.out.println("Data pages:");
        for(int blockNo: dataBlocks.keySet()) {
            DataBlockDump bd = dataBlocks.get(blockNo);
            System.out.format("%d: nextFree=%d, free=%d, numEntries=%d%n", 
                    bd.blockNo, bd.nextFree, bd.freeSpace, bd.numEntries);
        }
        
        long bytes = 0;
        for( FreeListEntry entry : freeList )
            bytes += entry.freeSpace;
        System.out.format("Free list: blocks=%d space=%d%n", freeList.size(), bytes);
        System.out.println();
    }
    
    public void printHeader() {
        System.out.format("RecordManager HeaderSize=%d BlockSize=%d FreeListThreshold=%d EntryExtra=%d PageExtra=%d%n", 
                headerSize, blockSize, freeListThreshold, ENTRY_EXTRA, PAGE_EXTRA);
        System.out.println(header.toString());
    }
    
}
