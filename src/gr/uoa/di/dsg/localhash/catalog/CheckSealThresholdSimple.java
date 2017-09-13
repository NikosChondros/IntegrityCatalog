package gr.uoa.di.dsg.localhash.catalog;

public class CheckSealThresholdSimple implements ICheckSealThreashold {
	/* (non-Javadoc)
	 * @see gr.uoa.di.dsg.localhash.catalog.ICheckSealThreashold#check(int, int)
	 */
	@Override
	public boolean check(int confirmed, int total) {
		return confirmed == total;
	}
}
