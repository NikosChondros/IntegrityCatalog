package gr.uoa.di.dsg.localhash.hostapp;

import java.io.Serializable;

public abstract class Message implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8288942667252126022L;

	protected abstract String getArguments();

	@Override
	public String toString() {
		return String.format("%s(%s)", getClass().getSimpleName(), this.getArguments());
	}
}
