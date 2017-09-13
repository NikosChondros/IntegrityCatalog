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
public class DataBlockEntry implements Comparable<DataBlockEntry> {
    public long recNo;
    public int position;
    public int length;
    public int realIndex;

    public DataBlockEntry(long recNo, int position, int length, int realIndex) {
        this.recNo = recNo;
        this.position = position;
        this.length = length;
        this.realIndex = realIndex;
    }

    @Override
    public int compareTo(DataBlockEntry t) {
        int ret = Integer.compare(this.position, t.position);
        if( ret == 0 ) //this one accounts for two elements at the same position, with the one having zero length. that should before the larger one
            ret = Integer.compare(this.length, t.length);
        if( ret == 0 ) //and this for the case where two zero length elements appear on the same data page
            ret = Long.compare(recNo, t.recNo);
        return ret;
    }
}
