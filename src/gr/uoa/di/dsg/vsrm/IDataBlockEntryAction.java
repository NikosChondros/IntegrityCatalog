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
public interface IDataBlockEntryAction {
    boolean process(int index, long recNo, int position, int length); //should return true if walking should continue
}
