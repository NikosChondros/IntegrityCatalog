/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.uoa.di.dsg.FileManager;

import java.nio.ByteBuffer;

/**
 *
 * @author nikos
 */
public class FileBlock {
    private int blockNo;
    private ByteBuffer buffer;

    public FileBlock(int blockNo, ByteBuffer buffer) {
        this.blockNo = blockNo;
        this.buffer = buffer;
    }
    
    public int getBlockNo() {
        return blockNo;
    }
    
    public ByteBuffer getBuffer() {
        return buffer;
    }
}
