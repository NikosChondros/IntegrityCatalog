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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

/**
 *
 * @author nikos
 */
public class MappedFileManager implements IFileManager {
    private static final int TARGET_INDIVIDUAL_MAPPING_SIZE = Integer.MAX_VALUE - 4095; //1048579; //4096; //2^30, i.e. 1 GiB
    private final int individualMappingSize; //4096; //Integer.MAX_VALUE - 4095; //2^30, i.e. 1 GiB
    private final int extentSize; 
    
    private final int headerSize;
    private final int blockSize;
    private FileChannel fc;

    private long alreadyMapped;
    ArrayList<MappedByteBuffer> buffers = new ArrayList<>();
    private MappedByteBuffer headerBuffer;
    
    public enum WritePolicy {
        ON_WRITE,
        ON_BATCH_END,
        ON_CLOSE
    }
    
    private final WritePolicy writePolicy;
//    private final byte[] emptyBlock;

    public MappedFileManager(String fileName, int headerSize, int blockSize, int extentSize, WritePolicy writePolicy) throws FileNotFoundException, IOException {
        this.headerSize = headerSize;
        this.blockSize = blockSize;
        this.extentSize = extentSize;
        this.writePolicy = writePolicy;

        //ensure a block is addressed in a single memory mapped buffer
        individualMappingSize = TARGET_INDIVIDUAL_MAPPING_SIZE - TARGET_INDIVIDUAL_MAPPING_SIZE % blockSize;
//        System.out.format("Ind map size=%d%n", individualMappingSize);
        if( blockSize > individualMappingSize )
            throw new IllegalArgumentException(String.format("Block size of %d is larger than individual mapping size of %d", blockSize, individualMappingSize));

        RandomAccessFile aFile;
        aFile = new RandomAccessFile(fileName, "rw");
        fc = aFile.getChannel();
        headerBuffer = fc.map(FileChannel.MapMode.READ_WRITE, 0, headerSize); //this will extend the file to at least headerSize size
        alreadyMapped = headerSize;
        
        //map as much of the file as possible
        long toMap = fc.size() - alreadyMapped;
        for(int i=0; toMap > 0; i++) {
            //System.out.format("map %d %d%n", i, alreadyMapped);
            long mapStart = (long) headerSize + (long) i * (long) individualMappingSize;
            int thisChunk = toMap > individualMappingSize ? individualMappingSize : (int) toMap;
            buffers.add(fc.map(FileChannel.MapMode.READ_WRITE, mapStart, thisChunk));
            toMap -= thisChunk;
            alreadyMapped += thisChunk;
        }
//        emptyBlock = new byte[blockSize];
    }
    
    @Override
    public int getHeaderSize() {
        return headerSize;
    }
    
    @Override
    public int getBlockSize() {
        return blockSize;
    }

    @Override
    public boolean isEmpty() throws IOException {
        return fc.size() <= headerSize;
    }

    @Override
    public ByteBuffer getHeaderBuffer() throws IOException {
//        extend(headerSize-1);
//        
//        if( headerBuffer == null )
//            headerBuffer = slice(buffers.get(0), 0, headerSize);
  
        return headerBuffer;
    }

    @Override
    public void readHeader() throws IOException {
        //nop
    }

    @Override
    public void writeHeader() throws IOException {
        //nop
    }

    @Override
    public FileBlock emptyBlock(int blockNo) throws IOException {
        if( blockNo == 31 )
            blockNo += 0;
        extend((long) headerSize + (long) (blockNo + 1) * blockSize - 1);
        ByteBuffer buffer = slice(blockNo);
        //buffer.put(emptyBlock);  //this is not needed as extents are initialized to 0 in file mappings
        return new FileBlock(blockNo, buffer);
    }

    @Override
    public void close() throws IOException {
        buffers.clear();
        fc.close();
        fc = null;
    }

    @Override
    public FileBlock readBlock(int blockNo) throws IOException {
        return new FileBlock(blockNo, slice(blockNo));
    }

    @Override
    public void releaseBlock(FileBlock block) {
        //nop
    }
    
    private void flush() {
        
    }

    @Override
    public void writeBlock(FileBlock block) throws IOException {
        if( writePolicy == WritePolicy.ON_WRITE ) {
            long relativeFilePos = (long) block.getBlockNo() * (long) blockSize;
            int bufferIndex = (int) (relativeFilePos / (long) individualMappingSize);
            buffers.get(bufferIndex).force();
        }
    }

    @Override
    public void beginBatch(String name) {
        //nop
    }

    @Override
    public void endBatch() {
        //nop
    }

    @Override
    public long getFileSize() throws IOException {
    	return fc.size();
    }
    
    @Override
    public void reportStatistics() {
        //TODO
    }
    
    private ByteBuffer slice(int blockNo)  {
        long offsetFromHeader = (long) blockNo * blockSize; 
        int bufferIndex = (int) (offsetFromHeader / individualMappingSize);
        int start = (int) (offsetFromHeader % individualMappingSize);
        return slice(buffers.get(bufferIndex), start, blockSize);
    }
    
    private ByteBuffer slice(MappedByteBuffer original, int start, int size) {
        return (ByteBuffer) ((ByteBuffer)(original.clear().position(start).limit(start+size))).slice();
    }
    
    private void extend(long lastAddressableByte) throws IOException {
        if( lastAddressableByte > alreadyMapped) {
            //file should be extended
            long newSize = headerSize + ((lastAddressableByte - headerSize) / extentSize) * extentSize;
            if( newSize < lastAddressableByte )
                newSize += extentSize;
            long missing = newSize - alreadyMapped;
            //extend the last buffer if needed
            int lastBufferIndex = buffers.size() - 1;
            
            if( lastBufferIndex >= 0 ) {
                MappedByteBuffer lastBuffer = buffers.get(lastBufferIndex);
                int thisMappingSize = lastBuffer.capacity();
                if( thisMappingSize < individualMappingSize ) {
                    //extend it up to INDIVIDUAL_MAPPING_SIZE
                    long mapStart = (long) headerSize + (long) lastBufferIndex * (long) individualMappingSize;
                    long todo = missing + thisMappingSize;
                    int thisChunk = todo > individualMappingSize ? individualMappingSize : (int) todo;
                    
                    //TODO: Delete the current mapping first?
                    buffers.set(lastBufferIndex, fc.map(FileChannel.MapMode.READ_WRITE, mapStart, thisChunk));
                    int diff = thisChunk - thisMappingSize;
                    missing -= diff;
                    alreadyMapped += diff;
                } 
            }
            while( missing > 0 ) {
                lastBufferIndex++;
                long mapStart = (long) headerSize + (long) lastBufferIndex * (long) individualMappingSize;
                int thisChunk = missing > individualMappingSize ? individualMappingSize : (int) missing;
                buffers.add(fc.map(FileChannel.MapMode.READ_WRITE, mapStart, thisChunk));
                missing -= thisChunk;
                alreadyMapped += thisChunk;
            }
        }
    }


}
