package gr.uoa.di.dsg.treap;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import edu.stanford.identiscape.util.ByteArrayRegion;
import edu.stanford.identiscape.util.Bytes;

public class ExistenceProof {
	private static ExistenceProof proof;
	protected static void reset() {
		proof = new ExistenceProof();
	}
	
	protected static void decide(boolean value) {
		proof.found = value;
	}
	
	protected static void feed(Node node, long snapshotId) {
		proof.keys.add(node.getKey());
		proof.payloads.add(node.getPayload(snapshotId));
	}
	
	protected static void feed(byte[] authenticator) {
		proof.childAuthenticators.add(authenticator);
	}
	
	protected static ExistenceProof conclude() {
		return proof;
	}
	
	private boolean found;
	private ArrayList<byte[]> keys = new ArrayList<>();
	private ArrayList<byte[]> payloads = new ArrayList<>();
	private ArrayList<byte[]> childAuthenticators = new ArrayList<>();
	
	
	private ExistenceProof() {
	}
	
	public boolean isMember() {
		return found;
	}
	
	public boolean validate(byte[] targetAuthenticator, byte[] key) {
		return validate(targetAuthenticator, new ByteArrayRegion(key));
	}
	
	public boolean validate(byte[] targetAuthenticator, ByteArrayRegion key) {
		if( childAuthenticators.size() == 0 )
			if( !found )
				return true; //as it should be
			else
				return false; //proof is crooked 
		//some sanity checks
		if( (keys.size() != childAuthenticators.size() - 1) || (keys.size() != payloads.size() ))
			return false;
		
		//pick up the last authenticator and its 
		int i = keys.size() - 1;
		
		ByteArrayRegion currentKey = new ByteArrayRegion(keys.get(i));
		ByteArrayRegion currentPayload = new ByteArrayRegion(payloads.get(i));
		
		int compareResult = Bytes.compare(key, currentKey);
		if( (compareResult == 0 && !found) || compareResult != 0 && found )
			return false;
		
		byte[] currentAuthenticator;
		if (compareResult < 0 )
			currentAuthenticator = Node.calculateAuthenticator(currentKey, currentPayload, childAuthenticators.get(i+1), childAuthenticators.get(i));
		else
			currentAuthenticator = Node.calculateAuthenticator(currentKey, currentPayload, childAuthenticators.get(i), childAuthenticators.get(i+1));
		
		while( i > 0 ) {
			i--;
			currentKey = new ByteArrayRegion(keys.get(i));
			currentPayload = new ByteArrayRegion(payloads.get(i));
			compareResult = Bytes.compare(key, currentKey);
			if( compareResult == 0 )
				//oops
				return false;
			else 
				if ( compareResult < 0 )
					currentAuthenticator = Node.calculateAuthenticator(currentKey, currentPayload, currentAuthenticator, childAuthenticators.get(i));
				else
					currentAuthenticator = Node.calculateAuthenticator(currentKey, currentPayload, childAuthenticators.get(i), currentAuthenticator);
		}
		
		return Bytes.compare(currentAuthenticator, 0, targetAuthenticator, 0, currentAuthenticator.length) == 0;
	}

	public NodePayload getPayloadOfKey() {
		return Node.decodePayload(payloads.get(payloads.size()-1));
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("Found=%b keys=%d ca=%d%n", found, keys.size(), childAuthenticators.size()));
		for( int i = 0; i < keys.size(); i++ ) {
			String sKey;
			try {sKey = new String(keys.get(i), "UTF-8"); }
			catch (UnsupportedEncodingException ex) {sKey = "ERROR";}
			sb.append(String.format("[%d] key=%s value=%s%n", i, sKey, Node.payloadToString(payloads.get(i))));
		}
		for( int i = 0; i < childAuthenticators.size(); i++ ) {
			sb.append(String.format("[%d] auth=%s%n", i, Bytes.toString(childAuthenticators.get(i))));
		}
		return sb.toString();
	}
}