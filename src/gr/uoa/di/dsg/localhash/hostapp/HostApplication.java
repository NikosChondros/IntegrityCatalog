/**
 * 
 */
package gr.uoa.di.dsg.localhash.hostapp;

import java.util.Set;

/**
 * @author nikos
 *
 */
public interface HostApplication {
	public void send(Node node, Message msg);
	public void schedule(Message msg);
	
	public void notifySealFinished(boolean status, String errorMessage);
	public void notifyRecoveryFinished(boolean status, String errorMessage);
	public void notifyVerificationFinished(boolean status, String errorMessage);
	
	public void setTimeout(int id, int duration, Object extra);
	public void cancelTimeout(int id, Object extra);

	public String getLocalCatalogPath();
	public String getRemoteCatalogPath(Node n);
	
	public Set<Node> getVerifiers();
	public Set<Node> getPreservers();
	public Set<Node> getPreservees();
	
}
