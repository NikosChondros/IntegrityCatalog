/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.uoa.di.dsg.vsrm;

/**
 *
 * @author nikos
 */
public class FreeListEntry {
    public int blockNo;
    public int freeSpace;
    public int nextInList;
    public FreeListEntry(int blockNo, int freeSpace, int nextInList) {
        this.blockNo = blockNo;
        this.freeSpace = freeSpace;
        this.nextInList = nextInList;
    }
}
