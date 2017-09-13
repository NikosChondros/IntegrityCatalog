/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.uoa.di.dsg.vsrm;

import java.nio.ByteBuffer;

/**
 *
 * @author nikos
 */
public class Header {
    public static final int MIN_SIZE = 48;
    
    public short signature = 0x5652; //VR
    public short formatVersion = 0;
    private int numBlocks = 0;
    private int firstFreeBlock = 0;
    private int lastDataBlock = 0;
    private long numRecordsInFile = 0;
    private long sumRecordBytes = 0;
    private int minRecordLength = Integer.MAX_VALUE;
    private int maxRecordLength = 0;
    private int numIndexBlocks = 0;
    private int numDataBlocks = 0;
    
    protected ByteBuffer buffer;
    protected boolean dirty;

    public Header(ByteBuffer buffer) {
        this.buffer = buffer;
    }
    
    public void toBuffer() {
        buffer.rewind();
        buffer.putShort(signature);
        buffer.putShort(formatVersion);
        buffer.putInt(numBlocks);
        buffer.putInt(firstFreeBlock);
        buffer.putInt(lastDataBlock);
        buffer.putLong(numRecordsInFile);
        buffer.putLong(sumRecordBytes);
        buffer.putInt(minRecordLength);
        buffer.putInt(maxRecordLength);
        buffer.putInt(numIndexBlocks);
        buffer.putInt(numDataBlocks);
    }
    
    public void fromBuffer() {
        buffer.rewind();
        signature = buffer.getShort();
        formatVersion = buffer.getShort();
        numBlocks = buffer.getInt();
        firstFreeBlock = buffer.getInt();
        lastDataBlock = buffer.getInt();
        numRecordsInFile = buffer.getLong();
        sumRecordBytes = buffer.getLong();
        minRecordLength = buffer.getInt();
        maxRecordLength = buffer.getInt();
        numIndexBlocks = buffer.getInt();
        numDataBlocks = buffer.getInt();
    }
    
    public void addRecord(int recSize) {
        numRecordsInFile += 1;
        sumRecordBytes += recSize;
        if( recSize < minRecordLength )
            minRecordLength = recSize;
        if( recSize > maxRecordLength )
            maxRecordLength = recSize;
        
      //if you want accurate header statistics, uncomment this.
      //*STATS*/dirty = true; 
    }
    
    public void delRecord(int recSize) {
        numRecordsInFile -= 1;
        sumRecordBytes -= recSize;
        //if you want accurate header statistics, uncomment this.
        //*STATS*/dirty = true; 
    }

    @Override
    public String toString() {
        return String.format(
                "Header: numBlocks=%d, firstFreeBlock=%d, lastDataBlock=%d, numDataBlocks=%d, numIndexBlocks=%d, numRecordsInFile=%d,sumRecordBytes=%d,minRecordLength=%d,maxRecordLength=%d", 
                numBlocks,
                firstFreeBlock,
                lastDataBlock,
                numDataBlocks,
                numIndexBlocks,
                numRecordsInFile,
                sumRecordBytes,
                minRecordLength,
                maxRecordLength
        );
    }
    
    protected int getNumBlocks() {
    	return numBlocks;
    }
    
    protected void setNumBlocks(int v) {
    	numBlocks = v;
    	dirty = true;
    }
    
    /**
     * 
     * @return the current value (before increasing it by 1)
     */
    protected int incrNumBlocks() {
    	dirty = true;
    	return numBlocks++;
    }

	int getFirstFreeBlock() {
		return firstFreeBlock;
	}

	void setFirstFreeBlock(int firstFreeBlock) {
		this.firstFreeBlock = firstFreeBlock;
    	dirty = true;
	}

	int getLastDataBlock() {
		return lastDataBlock;
	}

	void setLastDataBlock(int lastDataBlock) {
		this.lastDataBlock = lastDataBlock;
		dirty = true;
	}

	long getNumRecordsInFile() {
		return numRecordsInFile;
	}

	long getSumRecordBytes() {
		return sumRecordBytes;
	}

	int getMinRecordLength() {
		return minRecordLength;
	}

	int getMaxRecordLength() {
		return maxRecordLength;
	}

    void incrNumIndexBlocks() {
    	numIndexBlocks++;
    	dirty = true;
    }
    
    void setNumIndexBlocks(int v) {
    	numIndexBlocks = v;
    	dirty = true;
    }
    
    void incrNumDataBlocks() {
    	numDataBlocks++;
    	dirty = true;
    }

    void setNumDataBlocks(int v) {
    	numDataBlocks = v;
    	dirty = true;
    }
    
}
