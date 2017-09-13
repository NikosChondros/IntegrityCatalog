/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.uoa.di.dsg.vsrm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 *
 * @author nikos
 */
public class FreeListManager {
    private RecordManager host;
    private ArrayList<FreeListEntry> cachedList = new ArrayList<>();
    
    public FreeListManager(RecordManager host) {
        this.host = host;
    }
    
    public int findBlock(final int needed) throws IOException {
        final int ret[] = new int[1]; //darn hack required to pass back something

        walkFreeList(new IFreeListAction() {
            @Override
            public boolean process(FreeListEntry entry) {
                if( entry.freeSpace > needed ) { // first fit policy
                    ret[0] = entry.blockNo;
                    return false; //stop walking
                }
                return true; //keep walking
            }
        });
        
        return ret[0];
    }
    
    public void addBlock(DataBlock block) throws IOException {
        int blockFreeSpace = block.getGrossFreeSpace();
        if( blockFreeSpace < host.freeListThreshold )
            return;
        
        int current_head = host.header.getFirstFreeBlock();

        //update cache
        cachedList.add(0, new FreeListEntry(block.getBlockNo(), blockFreeSpace, current_head));
        //*DEBUG*/if( block.getBlockNo() == 40 )
            //*DEBUG*/System.out.format("ADD %d:%d%n", block.getBlockNo(), blockFreeSpace);
        //add it to the head
        //ALWAYS update next free block, to X or 0 (because it may come up again in the list)
        if( current_head > 0 ) { 
            block.setNextFreeBlock(current_head);
            host.writeBlock(block);
        }
        host.header.setFirstFreeBlock(block.getBlockNo());
        //*TRACE*/System.out.format("FL.Add b=%d free=%d next_free=%d header=%d%n", block.getBlockNo(), blockFreeSpace, current_head, host.header.firstFreeBlock);
    }
    
    public void checkUpdatedBlock(final DataBlock block, int previousFreeSpace) throws IOException {
        /*
        Parameters:
            is block already in cache?
            has block's free space expanded or shrinked?
            if shrinked, has it fell below threshold?
            if expanded, was it below threshold before and not over it?
        */
        //*DEBUG*/final String[] action = new String[1];
        final int newFreeSpace = block.getGrossFreeSpace();
        
        if( newFreeSpace == previousFreeSpace ) //if no change in free size, bail
        	return;
        
        if( previousFreeSpace >= host.freeListThreshold ) {
            //*DEBUG*/action[0] = "Already in free list:";
            //should already be in the cache
            if( newFreeSpace < host.freeListThreshold ) {
                //remove it from the list
                //*DEBUG*/action[0] += "Remove-";
                walkFreeList( new IFreeListAction() {
                    private FreeListEntry previous = null;
                    @Override
                    public boolean process(FreeListEntry entry) throws IOException {
                        if( entry.blockNo == block.getBlockNo() ) {
                            if( previous == null ) {
                                //*DEBUG*/action[0] += "update header";
                                host.header.setFirstFreeBlock(entry.nextInList);
                                //*TRACE*/System.out.format("FL.Del block=%d free=%d HEAD=%d%n", block.getBlockNo(), newFreeSpace, entry.nextInList);
                            } else {
                                //*DEBUG*/action[0] += "modify previous " + Integer.toString(previous.blockNo);
                                DataBlock prvBlock = new DataBlock(host.readBlock(previous.blockNo));
                                prvBlock.setNextFreeBlock(entry.nextInList);
                                host.writeBlock(prvBlock);
                                host.release(prvBlock);
                                //AND ALSO UPDATE CACHE!!!
                                previous.nextInList = entry.nextInList;
                                //*TRACE*/System.out.format("FL.Del block=%d free=%d prv_block=%d prv_block.next=%d%n", block.getBlockNo(), newFreeSpace, previous.blockNo, entry.nextInList);
                            }
                            //in any case, remove it from the list
                            cachedList.remove(entry);
                            return false; //stop walking
                        } else {
                            previous = entry;
                            return true; //keep walking
                        }
                    }
                });
            } else {
                //*DEBUG*/action[0] += "UPD Cache-";
                //update its size in the cache
                //do it only if it is already in the cache! (no walker here)
                for(FreeListEntry entry : cachedList )
                    if( entry.blockNo == block.getBlockNo() ) {
                        //*DEBUG*/action[0] += String.format("space %d->%d", entry.freeSpace, newFreeSpace);
                        //*TRACE*/System.out.format("FL.Upd block=%d new_size=%d old_size=%d%n", block.getBlockNo(), newFreeSpace, entry.freeSpace);
                        entry.freeSpace = newFreeSpace;
                        break;
                    }
            }
        } else {
            //*DEBUG*/action[0] = "New in free list:";
            //should not be in the cache
            if ( newFreeSpace >= host.freeListThreshold ) {
                //add it to the list of free blocks;
                //*DEBUG*/action[0] += "Cache add, space=" + Integer.toString(newFreeSpace);
                addBlock(block);
            }
        }
        //*DEBUG*/if( block.getBlockNo() == 40 )
           //*DEBUG*/System.out.format("CHECK UPDATED(%d, prv=%d, new=%d) %s%n", block.getBlockNo(), previousFreeSpace, newFreeSpace, action[0]);
    }
    
    private enum WalkerReturn {
        EMPTY, STOPPED, EXHAUSTED
    }
    
    /**
     * 
     * @param action
     * @return 0 when list is empty, 1 when action instructed walker to stop, 2 when free list was exhausted without action signaling to stop
     * @throws IOException 
     */
    private WalkerReturn walkFreeList(IFreeListAction action) throws IOException {
         FreeListEntry entry;
        if( cachedList.isEmpty() ) {
            //try to start over caching
            int first = host.header.getFirstFreeBlock();
            if( first == 0 ) //no free blocks
                return WalkerReturn.EMPTY;
            else { // fetch the first entry and put it in the cache
                ByteBuffer buffer;
                DataBlock block;
                block = new DataBlock(host.readBlock(first));
                cachedList.add(new FreeListEntry(block.getBlockNo(), block.getGrossFreeSpace(), block.getNextFreeBlock()));
                host.release(block);
            }
        }
        
        int i = 0, next;
        do {
            entry = cachedList.get(i);
            if( ! action.process(entry) ) //false means stop, nothing more
                return WalkerReturn.STOPPED;
            
            next = entry.nextInList;
            if( next > 0 && i == cachedList.size() - 1) { //if there are more free pages but we are at the end of the cache
                //fetch the next block
                ByteBuffer buffer;
                DataBlock block;
                block = new DataBlock(host.readBlock(next));
                cachedList.add(new FreeListEntry(next, block.getGrossFreeSpace(), block.getNextFreeBlock()));
                host.release(block);
            }
            i++;
        } while(next > 0);
        return WalkerReturn.EXHAUSTED;
    }
    
    protected ArrayList<FreeListEntry> dumpList() throws IOException {
        walkFreeList(new IFreeListAction() {
            @Override
            public boolean process(FreeListEntry entry) {
                return true; //keep walking
            }
        });
        return cachedList;
    }
}
