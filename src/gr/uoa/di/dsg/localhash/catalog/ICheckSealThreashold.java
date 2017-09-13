package gr.uoa.di.dsg.localhash.catalog;

public interface ICheckSealThreashold {

	public abstract boolean check(int confirmed, int total);

}