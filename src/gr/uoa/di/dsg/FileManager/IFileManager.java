/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.uoa.di.dsg.FileManager;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author nikos
 */
public interface IFileManager {

    int getBlockSize();

    int getHeaderSize();

    boolean isEmpty() throws IOException; //isEmpty must be called before getHeaderBuffer

    ByteBuffer getHeaderBuffer() throws IOException;

    void readHeader() throws IOException;
    
    void writeHeader() throws IOException;

    FileBlock emptyBlock(int blockNo) throws IOException;

    void close() throws IOException;

    FileBlock readBlock(int blockNo) throws IOException;

    void releaseBlock(FileBlock block);

    void writeBlock(FileBlock block) throws IOException;

    void beginBatch(String name);

    void endBatch();
    
    long getFileSize() throws IOException;

    /*TODO*/
    void reportStatistics();

    
}
