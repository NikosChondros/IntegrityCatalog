/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package gr.uoa.di.dsg.vsrm.dump;

import java.util.ArrayList;

/**
 *
 * @author nikos
 */
public class DataBlockDump {
    public int blockNo;
    public int nextFree;
    public int freeSpace;
    public int numEntries;
    public ArrayList<DataBlockEntryDump> entries = new ArrayList<>();
}
