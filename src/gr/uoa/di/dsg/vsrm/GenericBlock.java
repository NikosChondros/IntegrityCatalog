/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.uoa.di.dsg.vsrm;

import gr.uoa.di.dsg.FileManager.FileBlock;

import java.nio.ByteBuffer;

/**
 *
 * @author nikos
 */
public abstract class GenericBlock {
    private FileBlock block;

    public GenericBlock(FileBlock block) {
        this.block = block;
    }

    /**
     * @return the blockNo
     */
    public int getBlockNo() {
        return block.getBlockNo();
    }

    /**
     * @return the buffer
     */
    public ByteBuffer getBuffer() {
        return block.getBuffer();
    }
    
    public FileBlock getBlock() {
        return block; 
    }
}
