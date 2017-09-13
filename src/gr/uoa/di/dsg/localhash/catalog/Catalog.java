/**
 * 
 */
package gr.uoa.di.dsg.localhash.catalog;

import edu.stanford.identiscape.mappedMemory.MappedMemory;
import gr.uoa.di.dsg.FileManager.IFileManager;
import gr.uoa.di.dsg.FileManager.MappedFileManager;
import gr.uoa.di.dsg.localhash.catalog.messages.RecoverBeginMessage;
import gr.uoa.di.dsg.localhash.catalog.messages.RecoverDataMessage;
import gr.uoa.di.dsg.localhash.catalog.messages.RecoverEndOfDataMessage;
import gr.uoa.di.dsg.localhash.catalog.messages.RecoverGetNextMessage;
import gr.uoa.di.dsg.localhash.catalog.messages.RecoverVersionReplyMessage;
import gr.uoa.di.dsg.localhash.catalog.messages.RecoverVersionRequestMessage;
import gr.uoa.di.dsg.localhash.catalog.messages.RemoteCatalogSyncMessage;
import gr.uoa.di.dsg.localhash.catalog.messages.StoreMessage;
import gr.uoa.di.dsg.localhash.catalog.messages.StoreReplyMessage;
import gr.uoa.di.dsg.localhash.catalog.messages.StoredVersionReplyMessage;
import gr.uoa.di.dsg.localhash.catalog.messages.StoredVersionRequestMessage;
import gr.uoa.di.dsg.localhash.catalog.messages.UpdateBeginMessage;
import gr.uoa.di.dsg.localhash.catalog.messages.UpdateDataMessage;
import gr.uoa.di.dsg.localhash.catalog.messages.UpdateEndOfDataMessage;
import gr.uoa.di.dsg.localhash.catalog.messages.UpdateGetNextMessage;
import gr.uoa.di.dsg.localhash.catalog.messages.VersionResetMessage;
import gr.uoa.di.dsg.localhash.catalog.messages.VersionResetReplyMessage;
import gr.uoa.di.dsg.localhash.hostapp.HostApplication;
import gr.uoa.di.dsg.localhash.hostapp.Message;
import gr.uoa.di.dsg.localhash.hostapp.MessageProcessor;
import gr.uoa.di.dsg.localhash.hostapp.Node;
import gr.uoa.di.dsg.treap.AuthenticatorCacheManagerAlways;
import gr.uoa.di.dsg.treap.ExistenceProof;
import gr.uoa.di.dsg.treap.NodeStorageManagerVSRM;
import gr.uoa.di.dsg.treap.SnapshotExtractorFunctions;
import gr.uoa.di.dsg.treap.Tree;
import gr.uoa.di.dsg.vsrm.RecordManager;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.Vector;

import javax.xml.bind.DatatypeConverter;

/**
 * @author Nikos Chondros
 *
 */
public class Catalog implements MessageProcessor {
	private class PreserveData {
		public String treePath;
		public long targetSnapshotId;
		public byte[] authenticator;
		public long currentSnapshotId;
		public PreserveData(String treePath, long targetSnapshotId, byte[] authenticator, long currentSnapshotId) {
			super();
			this.treePath = treePath;
			this.targetSnapshotId = targetSnapshotId;
			this.authenticator = authenticator;
			this.currentSnapshotId = currentSnapshotId;
		}
	}
	/*Configuration*/
	private static final int STORE_TIMEOUT_DURATION = 60; //seconds
	private static final int VERIFY_TIMEOUT_DURATION = 60; //seconds
	private static final int RECOVER_PHASE1_TIMEOUT_DURATION = 60; //seconds
	private static final int RECOVER_PHASE2_TIMEOUT_DURATION = 60; //seconds
	private static final int RECOVER_PHASE3_TIMEOUT_DURATION = 60; //seconds
	
	private static final ICheckSealThreashold checkSealThreshold = new CheckSealThresholdSimple();
	private static final IPerformVerify performVerify = new PerformVerifySimple();
	private static final IChooseRecoverVersion chooseRecoverVersion = new ChooseRecoverVersionSimple();
	
	/*Constants*/
	private static final String DATUM_ENCODING = "US-ASCII";
	private static final int SEAL_TIMEOUT_ID = 1;
	private static final int VERIFY_TIMEOUT_ID = 2;
	private static final int RECOVER_PHASE1_TIMEOUT_ID = 3;
	private static final int RECOVER_PHASE2_TIMEOUT_ID = 4;
	private static final int RECOVER_PHASE3_TIMEOUT_ID = 5;
	private static final String catalogTreeFile = "BTree.dat";
	private static final String catalogSkipListFile = "Skiplist.dat";
	
	//debugging aids
	public static Map<Integer, String> timeoutNames;
	static {
		timeoutNames = new HashMap<>();
		timeoutNames.put(SEAL_TIMEOUT_ID, "SEAL_TIMEOUT");
		timeoutNames.put(VERIFY_TIMEOUT_ID, "VERIFY_TIMEOUT");
		timeoutNames.put(RECOVER_PHASE1_TIMEOUT_ID, "RECOVER_PHASE1_TIMEOUT");
		timeoutNames.put(RECOVER_PHASE2_TIMEOUT_ID, "RECOVER_PHASE2_TIMEOUT");
		timeoutNames.put(RECOVER_PHASE3_TIMEOUT_ID, "RECOVER_PHASE3_TIMEOUT");
	}
	
	/*Instance variables*/
	private Tree localTree;
	private HostApplication host;
	private boolean stable = true;
	
	/**
	 * a sealNodeLog is a map from Node to Boolean, tracking the response from each Node
	 * a sealLog is a map from SnapshotData to the corresponding sealNodeLog, alloqing
	 * multiple seal operations to be active at any given time
	*/
	private Map<String, Map<Node, Boolean>> sealLog = new HashMap<>();
	
	/**
	 * verifyNodeLog is a map from  Node to SealData, tracking the response from each Node
	*/
	private Map<Node, SnapshotData> verifyNodeLog = new HashMap<>();
	
	/**
	 * recoverNodeLog is a map from Node to SealData, tracking the response from each Node
	*/
	private Map<Node, SnapshotData> recoverNodeLog = new HashMap<>();
	
	/**
	 * preserveNodeData is a map from Node to the PreserveData structure, allowing access to its fields upon receiving the replies from the remote node
	*/
	private Map<Node, PreserveData> preserveNodeData = new HashMap<>();
	
	/**
	 * saved state to consult upon completion of each version recovery
	 */
	private PreserveData recoverState; 
	/**
	 * 
	 * @param host The object representing the host application
	 */
	public Catalog(HostApplication host) throws Exception {
		this.host = host;
		
		localTree = openCatalog(host.getLocalCatalogPath());
	}
	
	private void zapLocalCatalog() throws Exception {
		localTree.close();
		String rootPath = host.getLocalCatalogPath();
		deleteFiles(rootPath + catalogTreeFile, rootPath + catalogSkipListFile);
		localTree = openCatalog(rootPath);
	}
	
	private void zapRemoteCatalog(Node source) {
		String rootPath = host.getRemoteCatalogPath(source);
		deleteFiles(rootPath + catalogTreeFile, rootPath + catalogSkipListFile);
	}
	
	private static void deleteFiles(String treeFileName, String skipFileName) {
		delete(new File(treeFileName));
		delete(new File(skipFileName));
	}
	
	private static void delete(File f) {
		if (f.isDirectory()) {
			for (File c : f.listFiles())
				delete(c);
		}
		f.delete();
	}
	
	private static boolean deleteFilesIfOneMissing(String treeFileName, String skipFileName) {
		File bf = new File(treeFileName);
		File sf = new File(skipFileName);
		if( (! (bf.exists() && sf.exists())) ) {
			delete(bf);
			delete(sf);
			return true;
		} else
			return false;
	}

    private NodeStorageManagerVSRM getSM(String rmFilename, String skipListFilename) throws Exception {
		//instantiate a VSRM
		IFileManager fm;
		fm = new MappedFileManager(rmFilename, 1024, 64 * 1024, 64 * 64 *1024, MappedFileManager.WritePolicy.ON_CLOSE);
		RecordManager rm = new RecordManager(fm, 100);
		
		MappedMemory mm = new MappedMemory(skipListFilename, 10);
		
		return new NodeStorageManagerVSRM(rm, mm);
    }
    
	private Tree openCatalog(String rootPath) throws Exception {
		Tree tree;
		
		String myCatalogTreeFile = rootPath + catalogTreeFile;
		String myCatalogSkipListFile = rootPath + catalogSkipListFile;
		deleteFilesIfOneMissing(myCatalogTreeFile, myCatalogSkipListFile);
		// Create the tree
		tree = new Tree(getSM(myCatalogTreeFile,myCatalogSkipListFile), new AuthenticatorCacheManagerAlways());
		return tree;
	}
	
	private byte[] getAuthenticator(Tree tree) throws IOException {
		return tree.getRootAuthenticator(tree.getLastClosedSnapshotId());
	}
	
	/*
	 *************************************************************************************** 
	 ******************************** High level get()+put() *******************************
	 *************************************************************************************** 
	 */
	/**
	 * Adds a key value pair to the catalog
	 * 
	 * @param key An arbitrary length string. Should not exist in the catalog already.
	 * @param value An arbitrary length string.
	 * @throws Exception 
	 */
	public void put(String key, String value) throws Exception {
		byte[] dvalue;
		try {
			dvalue = value.getBytes(DATUM_ENCODING);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("System error, unsupported encoding: " + DATUM_ENCODING);
		}
		put(key, dvalue);
	}
	
	/**
	 * Amends the value of a given key with the extra data passed
	 * 
	 * @param key An arbitrary length string. Should not exist in the catalog already.
	 * @param value An arbitrary length string.
	 * @throws IOException 
	 */
	public void amend(String key, String value) throws IOException {
		byte[] dvalue;
		try {
			dvalue = value.getBytes(DATUM_ENCODING);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("System error, unsupported encoding: " + DATUM_ENCODING);
		}
		amend(key, dvalue);
	}
	
	/**
	 * Obtains the value of a given key.
	 * The returned value is first verified
	 * @param key
	 * @return A string with the current value
	 * @throws IOException 
	 */
	public String get(String key) throws IOException {
		byte[] dvalue = getBytes(key);
		try {
			String ret = new String(dvalue, DATUM_ENCODING);
			return ret;
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("System error, unsupported encoding: " + DATUM_ENCODING);
		}
	}
	
	/*
	 *************************************************************************************** 
	 ******************************** Low level get()+put() ********************************
	 *************************************************************************************** 
	 */
	/**
	 * Adds a key value pair to the catalog
	 * 
	 * @param key Can be an arbitrary length string. It will be hashed with the selected hash algorithm 
	 * @param value Must be a string limited in length by the available space in MRBBTree (DVALUESIZE)
	 * @throws Exception 
	 */
	public void put(String key, byte[] dvalue) throws Exception {
		if( ! stable ) {
			throw new RuntimeException("You are trying to put new data to the Catalog while Seal is still in progress");
		}
		byte[] dkey;
		try {
			dkey = key.getBytes(DATUM_ENCODING);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("System error, unsupported encoding: " + DATUM_ENCODING);
		}
		localTree.insert(dkey, dvalue);
	}
	
	/**
	 * Amends the value of a given key with the extra data passed
	 * 
	 * @param key Can be an arbitrary length string.  
	 * @param value The value to append to the existing one
	 * @throws IOException 
	 */
	public void amend(String key, byte[] valueToAppend) throws IOException {
		if( ! stable ) {
			throw new RuntimeException("You are trying to amend data in the Catalog while Seal is still in progress");
		}
		byte[] dkey;
		try {
			dkey = key.getBytes(DATUM_ENCODING);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("System error, unsupported encoding: " + DATUM_ENCODING);
		}
		
		byte[] authenticator = getAuthenticator(localTree);

		ExistenceProof proof = localTree.isMember(localTree.getLastClosedSnapshotId(), dkey);
		if( ! proof.validate(authenticator, dkey) ) 
			throw new RuntimeException("Tree is corrupt, could not validate proof on Catalog.get()");
		
		if( ! proof.isMember() )
			throw new RuntimeException("Key not found");
		
		byte[] currentValue = proof.getPayloadOfKey().value;
		byte[] dNewValue = new byte[currentValue.length + valueToAppend.length];
		System.arraycopy(currentValue, 0, dNewValue,  0, currentValue.length);
		System.arraycopy(valueToAppend, 0, dNewValue, currentValue.length, valueToAppend.length);
		
		localTree.update(dkey, dNewValue);
	}
	
	/**
	 * Obtains the value of a given key.
	 * The returned value is first verified
	 * @param key
	 * @return A byte[] with the current value 
	 * @throws IOException 
	 */
	public byte[] getBytes(String key) throws IOException {
		if( ! stable ) {
			throw new RuntimeException("You are trying to retrieve data from the Catalog while Seal is still in progress");
		}
		byte[] dkey;
		try {
			dkey = key.getBytes(DATUM_ENCODING);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("System error, unsupported encoding: " + DATUM_ENCODING);
		}
		
		byte[] authenticator = getAuthenticator(localTree);

		ExistenceProof proof = localTree.isMember(localTree.getLastClosedSnapshotId(), dkey);
		if( ! proof.validate(authenticator, dkey) ) 
			throw new RuntimeException("Tree is corrupt, could not validate proof on Catalog.get()");
		
		if( ! proof.isMember() )
			return null;
		else
			return proof.getPayloadOfKey().value;
	}
	
	
	public boolean isStable() {
		return stable;
	}
	
	/*
	 **************************************************************************************** 
	 ************************************* Seal+helpers *************************************
	 **************************************************************************************** 
	 */
	
	private void sendVersionNotification(SnapshotData sd, Message msg, int timeoutId, int timeoutDuration) {
		Map<Node, Boolean> sealNodeLog = new HashMap<>();
		for( Node node : host.getVerifiers() ) {
			host.send(node, msg);
			sealNodeLog.put(node, false);
		}
		sealLog.put(sd.toString(), sealNodeLog);
		host.setTimeout(timeoutId, timeoutDuration, sd);
	}
	
	public void seal() throws IOException, InterruptedException {
		localTree.closeSnapshot();
		stable = false;
		long snapshotId = localTree.getLastClosedSnapshotId();
		byte[] authenticator = getAuthenticator(localTree);
		StoreMessage msg = new StoreMessage(snapshotId, authenticator);
		SnapshotData sd = new SnapshotData(snapshotId, authenticator);
		sendVersionNotification(sd,msg, SEAL_TIMEOUT_ID, STORE_TIMEOUT_DURATION);
	}
	
	private void onStore(StoreMessage msg, Node source) {
		if( putRemoteVersion(source, msg.getSnapshotId(), msg.getAuthenticator(), false) ) {
			host.send(source, new StoreReplyMessage(msg.getSnapshotId(), msg.getAuthenticator()));
			if( host.getPreservees().contains(source) )
				host.schedule(new RemoteCatalogSyncMessage(source, msg.getSnapshotId(), msg.getAuthenticator()));
		}
	}

	
	private void onStoreReply(StoreReplyMessage msg, Node source) {
		SnapshotData sd = new SnapshotData(msg.getSnapshotId(), msg.getAuthenticator());
		Map<Node, Boolean> sealNodeLog = sealLog.get(sd.toString());
		//make sure we have an open seal in progress
		if( sealNodeLog != null ) {
			//make sure this reply was solicited and not a duplicate
			if( sealNodeLog.containsKey(source) && ! sealNodeLog.get(source)) {
				sealNodeLog.put(source, true);
				checkSealDone(sd);
			}
		}
	}
	
	private String getRemoteVersionHistoryFile(Node node) {
		String folder = host.getRemoteCatalogPath(node);
		String file = folder + "version.history";
		return file;
	}
	
	private boolean putRemoteVersion(Node source, long snapshotId, byte[] authenticator, boolean force) {
		boolean ret = false;
		String file = getRemoteVersionHistoryFile(source);
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(file));
		} catch(Exception ex) {
			//ignore
		}
		long latest = Long.parseLong(prop.getProperty("latest", "0"));
		if( force || (snapshotId > latest) ) {
			String key = Long.toString(snapshotId);
			String value = DatatypeConverter.printHexBinary(authenticator);
			prop.setProperty(key, value);
			prop.setProperty("latest", key);
			ret = true;
			
			try {
				prop.store(new FileOutputStream(file), null);
			} catch(Exception ex) {
				throw new RuntimeException(ex);
			}
		}
		return ret;
	}
	
	protected SnapshotData getRemoteVersion(Node source) { //protected just for the test cases
		SnapshotData ret = null;
		String file = getRemoteVersionHistoryFile(source);
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(file));
			String latest = prop.getProperty("latest", "0");
			if( ! latest.equals("0") ) {
				String value = prop.getProperty(latest);
				byte[] authenticator = DatatypeConverter.parseHexBinary(value);
				long snapshotId = Long.parseLong(latest);
				ret = new SnapshotData(snapshotId, authenticator);
			}
		} catch(Exception ex) {
			//ignore
		}
		return ret;
	}
	
	private void sealTimeoutHandler(SnapshotData sd) {
		if( ! checkSealDone(sd) ) {
			sealLog.remove(sd.toString());
			host.notifySealFinished(false, String.format("Catalog snapshot with id %d was not confirmed by enough verifiers at seal", sd.snapshotId));
			stable = true;
		}
	}
	
	private boolean checkSealDone(SnapshotData sd) {
		boolean ret = false;
		Map<Node, Boolean> sealNodeLog = sealLog.get(sd.toString());
		if( sealNodeLog == null )
			return ret;
		int count = 0;
		for( boolean value : sealNodeLog.values() )
			if( value )
				count++;
		if( checkSealThreshold.check(count, sealNodeLog.size()) ) {
			//we are officially done
			host.cancelTimeout(SEAL_TIMEOUT_ID, sd); //this call may try to cancel a just-fired timeout if called from the timeout handler
			sealLog.remove(sd.toString()); //This removes it before they all respond!!!
			stable = true;
			ret = true;
			host.notifySealFinished(true, "");
		}
		return ret;
	}
	
	/*
	 ****************************************************************************************** 
	 ************************************* Verify+helpers *************************************
	 ****************************************************************************************** 
	 */
	public void verify() {
		StoredVersionRequestMessage msg = new StoredVersionRequestMessage();
		
		verifyNodeLog.clear();
		for( Node node : host.getVerifiers() ) {
			host.send(node, msg);
			verifyNodeLog.put(node, null);
		}
		host.setTimeout(VERIFY_TIMEOUT_ID, VERIFY_TIMEOUT_DURATION, null);
	}
	
	private void onStoredVersionRequest(StoredVersionRequestMessage msg, Node source) {
		SnapshotData sd = getRemoteVersion(source);
		if( sd != null )
			host.send(source, new StoredVersionReplyMessage(sd.snapshotId, sd.authenticator));
	}

	private void onStoredVersionReply(StoredVersionReplyMessage msg, Node source) throws IOException {
		SnapshotData sd = new SnapshotData(msg.getSnapshotId(), msg.getAuthenticator());
		if( verifyNodeLog.containsKey(source) && verifyNodeLog.get(source) == null ) {
			verifyNodeLog.put(source, sd);
			int replies = 0;
			for( SnapshotData entry : verifyNodeLog.values() )
				if( entry != null )
					replies++;
			if( replies == host.getVerifiers().size() )
				checkVerifyResult(true);
		}
	}
	
	private void verifyTimeoutHandler() throws IOException {
		checkVerifyResult(false);
	}
	
	private boolean checkVerifyResult(boolean allReplied) throws IOException {
		//in any case, cancel the timeout
		host.cancelTimeout(VERIFY_TIMEOUT_ID, null);
		
		/*
		 * We have a series of SnapshotData elements, with possibly conflicting authenticators.
		 * We also have our local view of things, ie another SnapshotData structure
		 */
		
		// Sum the votes by version and authenticator 
		TreeMap<Long, TreeMap<ByteArrayWrapper, Integer>> votes = new TreeMap<>();
		int voteCount = 0; //this counts the number of replies we received
		for( SnapshotData sd : verifyNodeLog.values() ) {
			if( sd != null ) {
				voteCount++;
				if( ! votes.keySet().contains(sd.snapshotId) ) //make sure an entry exists for this snapshotId
					votes.put(sd.snapshotId, new TreeMap<ByteArrayWrapper, Integer>());
				TreeMap<ByteArrayWrapper, Integer> versionData = votes.get(sd.snapshotId); //and obtain this entry
				ByteArrayWrapper vauthenticator = new ByteArrayWrapper(sd.authenticator); //wrap byte[] to class
				if( ! versionData.keySet().contains(vauthenticator) ) //make sure an entry exists for this digest (of current snapshotId)
					versionData.put(vauthenticator, 0);
				
				versionData.put(vauthenticator, versionData.get(vauthenticator)+1); //increase the number of votes by one
			}
		}
		//get our view of the snapshot
		long snapshotId = localTree.getLastClosedSnapshotId();
		byte[] authenticator = getAuthenticator(localTree);
		SnapshotData currentSD = new SnapshotData(snapshotId, authenticator);
		
		//call the designated verifier 
		boolean verified = performVerify.verify(votes, voteCount, currentSD);

		verifyNodeLog.clear();
		String errorMessage = "";
		if( ! verified ) {
			if( allReplied )
				errorMessage = "Failed to verify catalog even though all peers replied";
			else
				errorMessage = String.format("Failed to verify catalog, timeout while %d peers replied", voteCount);
		}
		host.notifyVerificationFinished(verified, errorMessage);
		return verified;
	}
	
	/*
	 ******************************************************************************************* 
	 ************************************* Recover+helpers *************************************
	 ******************************************************************************************* 
	 */
	//runs on local node
	public void recover() {
		/*
		 * This function "restores" the catalog from a "preserver"
		 * Another round of messages is exchanged to identify the best candidate, and then the restore actually starts.
		 */
		RecoverVersionRequestMessage msg = new RecoverVersionRequestMessage();
		
		recoverNodeLog.clear();
		for( Node node : host.getPreservers() ) {
			host.send(node, msg);
			recoverNodeLog.put(node, null);
		}
		host.setTimeout(RECOVER_PHASE1_TIMEOUT_ID, RECOVER_PHASE1_TIMEOUT_DURATION, null);
	}
	
	//runs on remote node
	private void onRecoverVersionRequest(RecoverVersionRequestMessage msg, Node source) throws Exception {
		try (Tree remoteTree = openCatalog(host.getRemoteCatalogPath(source))) {
			long snapshotId = remoteTree.getLastClosedSnapshotId();
			byte[] authenticator = getAuthenticator(remoteTree);
			host.send(source, new RecoverVersionReplyMessage(snapshotId, authenticator));
		}
	}
	
	//runs on local node
	private void onRecoverVersionReply(RecoverVersionReplyMessage msg, Node source) throws Exception {
		SnapshotData sd = new SnapshotData(msg.getSnapshotId(), msg.getAuthenticator());
		if( recoverNodeLog.containsKey(source) && recoverNodeLog.get(source) == null ) {
			recoverNodeLog.put(source, sd);
			int replies = 0;
			for( SnapshotData entry : recoverNodeLog.values() )
				if( entry != null )
					replies++;
			if( replies == host.getPreservers().size() )
				checkRecoverResult();
			
		}
	}
	
	//runs on local node
	private void recoverPhase1TimeoutHandler() throws Exception {
		checkRecoverResult();
	}
	
	//runs on local node
	private boolean checkRecoverResult() throws Exception {
		host.cancelTimeout(RECOVER_PHASE1_TIMEOUT_ID, null);
		
		/*
		 * We have a series of SnapshotData elements, with possibly conflicting authenticators.
		 */
		
		// Sum the votes by version and authenticator 
		TreeMap<Long, TreeMap<ByteArrayWrapper, Integer>> votes = new TreeMap<>();
		int voteCount = 0;
		for( SnapshotData sd: recoverNodeLog.values() ) {
			if( sd != null ) {
				voteCount++;
				if( ! votes.keySet().contains(sd.snapshotId) ) //make sure an entry exists for this snapshotId
					votes.put(sd.snapshotId, new TreeMap<ByteArrayWrapper, Integer>());
				TreeMap<ByteArrayWrapper, Integer> versionData = votes.get(sd.snapshotId); //and obtain it
				ByteArrayWrapper vauthenticator = new ByteArrayWrapper(sd.authenticator); //wrap byte[] to wrapper class
				if( ! versionData.keySet().contains(vauthenticator) ) //make sure an entry exists for this digest (of current snapshotId)
					versionData.put(vauthenticator, 0);
				
				versionData.put(vauthenticator, versionData.get(vauthenticator)+1); //increase the number of votes by one
			}
		}
		//call the designated chooser 
		SnapshotData chosenSD = chooseRecoverVersion.choose(votes, voteCount);
		if( chosenSD != null ) {
			recoverState = new PreserveData(null, chosenSD.snapshotId, chosenSD.authenticator, 0L);
			//now choose a node to recover from
			Vector<Node> nodes = new Vector<>();
			for( Node node: recoverNodeLog.keySet() ) {
				SnapshotData sd = recoverNodeLog.get(node);
				if( sd.snapshotId == chosenSD.snapshotId )
					if( new ByteArrayWrapper(sd.authenticator).equals(new ByteArrayWrapper(chosenSD.authenticator) ))
						nodes.add(node);
			}
			int choice = 0;
			if( nodes.size() > 1 ) {
				//choose one at random
				Random rand = new Random();
				choice = rand.nextInt(nodes.size());
			}
			zapLocalCatalog();
			host.setTimeout(RECOVER_PHASE2_TIMEOUT_ID, RECOVER_PHASE2_TIMEOUT_DURATION, null);
			host.send(nodes.get(choice), new RecoverBeginMessage(1));
			recoverNodeLog.clear();
			return true;
		} else {
			host.notifyRecoveryFinished(false, String.format("Failed to choose peer to recover catalog from, %d replied of %d total", voteCount, recoverNodeLog.size()));
			recoverNodeLog.clear();
			return false;
		}
	}
	
	//runs on local node
	private void recoverPhase2TimeoutHandler() {
		host.notifyRecoveryFinished(false, "Timeout error on recover phase 2 (restoring pages of the catalog)");
	}
	
	//runs on local node
	private void onRecoverData(RecoverDataMessage msg, Node source) throws IOException {
		host.cancelTimeout(RECOVER_PHASE2_TIMEOUT_ID, null);
		String filename = SnapshotExtractorFunctions.fileNameOfExtract(host.getLocalCatalogPath() + catalogTreeFile, msg.getSnapshotId());
		
		binaryUpdateBlock(filename, msg.getBlockNumber(), msg.getBlockData());
		host.send(source, new RecoverGetNextMessage(msg.getSnapshotId(), msg.getBlockNumber()+1));
		host.setTimeout(RECOVER_PHASE2_TIMEOUT_ID, RECOVER_PHASE2_TIMEOUT_DURATION, null);
	}
	
	//runs on local node
	private void onRecoverEndOfData(RecoverEndOfDataMessage msg, Node source) throws Exception {
		host.cancelTimeout(RECOVER_PHASE2_TIMEOUT_ID, null);
		String treePath = host.getLocalCatalogPath();
		SnapshotExtractorFunctions.apply(localTree, SnapshotExtractorFunctions.fileNameOfExtract(treePath + catalogTreeFile, msg.getSnapshotId()));
		localTree.closeSnapshot();
		long ourSnapshotId = localTree.getLastClosedSnapshotId();
		long targetSnapshotId = recoverState.targetSnapshotId;
		
		if( ourSnapshotId < targetSnapshotId ) {
			//If we are still behind, schedule another step to catch up
			host.send(source, new RecoverBeginMessage(ourSnapshotId + 1));
			host.setTimeout(RECOVER_PHASE2_TIMEOUT_ID, RECOVER_PHASE2_TIMEOUT_DURATION, null);
		} else {
			//*TODO: Verify authenticator matches with requested one
			recoverState = null;
			//host.notifyRecoveryFinished(true);
			SnapshotData sd = new SnapshotData(ourSnapshotId, getAuthenticator(localTree));
			sendVersionNotification(sd, new VersionResetMessage(sd.snapshotId, sd.authenticator), RECOVER_PHASE3_TIMEOUT_ID, RECOVER_PHASE3_TIMEOUT_DURATION);
		}
	}
	
	//runs on remote node
	private void onRecoverBegin(RecoverBeginMessage msg, Node source) throws Exception {
		String treePath = host.getRemoteCatalogPath(source);
		String treeFileName = treePath + catalogTreeFile;
		String snpashotExtractFilename = SnapshotExtractorFunctions.fileNameOfExtract(treeFileName, msg.getSnapshotId());
		if (! new File(snpashotExtractFilename).exists()) {
			Tree remoteTree = openCatalog(treePath);
			remoteTree.extractSnapshot(treeFileName, msg.getSnapshotId());
			remoteTree.close();
		}
		TreeExtractBlockInfo b = getChunk(snpashotExtractFilename, 0L);
		host.send(source, new RecoverDataMessage(msg.getSnapshotId(), b.number, b.data));
	}
	
	//runs on remote node
	private void onRecoverGetNext(RecoverGetNextMessage msg, Node source) throws IOException {
		String treePath = host.getRemoteCatalogPath(source);
		String treeFileName = treePath + catalogTreeFile;
		String snpashotExtractFilename = SnapshotExtractorFunctions.fileNameOfExtract(treeFileName, msg.getSnapshotId());
		TreeExtractBlockInfo b = getChunk(snpashotExtractFilename, msg.getBlockNumber());
		if( b == null )
			host.send(source, new RecoverEndOfDataMessage(msg.getSnapshotId()));
		else
			host.send(source, new RecoverDataMessage(msg.getSnapshotId(), b.number, b.data));
	}
	
	//runs on remote node
	private void onVersionReset(VersionResetMessage msg, Node source) {
		SnapshotData current = getRemoteVersion(source);
		if( putRemoteVersion(source, msg.getSnapshotId(), msg.getAuthenticator(), true) ) {
			host.send(source, new VersionResetReplyMessage(msg.getSnapshotId(), msg.getAuthenticator()));
			//If what we have is newer than what the peer currently reports
			if( host.getPreservees().contains(source) && current.snapshotId > msg.getSnapshotId() )
				//erase the catalog
				zapRemoteCatalog(source);
				//and fetch it again
				host.schedule(new RemoteCatalogSyncMessage(source, msg.getSnapshotId(), msg.getAuthenticator()));
		}
	}

	//runs on local node
	private void onVersionResetReply(VersionResetReplyMessage msg, Node source) {
		SnapshotData sd = new SnapshotData(msg.getSnapshotId(), msg.getAuthenticator());
		Map<Node, Boolean> sealNodeLog = sealLog.get(sd.toString());
		//make sure we have an open seal in progress
		if( sealNodeLog != null ) {
			//make sure this reply was solicited and not a duplicate
			if( sealNodeLog.containsKey(source) && ! sealNodeLog.get(source)) {
				sealNodeLog.put(source, true);
				checkVersionResetDone(sd);
			}
		}
	}
	
	private void recoverPhase3TimeoutHandler(SnapshotData sd) {
		if( ! checkVersionResetDone(sd) ) {
			sealLog.remove(sd.toString());
			host.notifyRecoveryFinished(false, String.format("Catalog snapshot with id %d was not confirmed by enough verifiers at version reset", sd.snapshotId));
			stable = true;
		}
	}
	
	private boolean checkVersionResetDone(SnapshotData sd) {
		boolean ret = false;
		Map<Node, Boolean> sealNodeLog = sealLog.get(sd.toString());
		if( sealNodeLog == null )
			return ret;
		int count = 0;
		for( boolean value : sealNodeLog.values() )
			if( value )
				count++;
		if( checkSealThreshold.check(count, sealNodeLog.size()) ) {
			//we are officially done
			host.cancelTimeout(RECOVER_PHASE3_TIMEOUT_ID, sd); //this call may try to cancel a just-fired timeout if called from the timeout handler
			sealLog.remove(sd.toString()); //This removes it before they all respond!!!
			//stable = true;
			ret = true;
			host.notifyRecoveryFinished(true, "");
		}
		return ret;
	}
	
	/*
	 ******************************************************************************************* 
	 ************************************* Preserve+helpers ************************************
	 ******************************************************************************************* 
	 */
	//runs on remote node, scheduled via the scheduler (sent to self, not sent over the wire)
	private void onRemoteCatalogSync(RemoteCatalogSyncMessage msg) throws Exception {
		String treePath = host.getRemoteCatalogPath(msg.getSource());
		Tree remoteTree = openCatalog(treePath);
		//should we even bother?
		long ourSnapshotId = remoteTree.getLastClosedSnapshotId();
		long targetSnapshotId = msg.getSnapshotId(); 
		remoteTree.close();
		if( ourSnapshotId >= targetSnapshotId ) {
			return;
		}
		//begin an update for 'ourSnapshotId' to reach msg.getSnapshotId(). Start with ourSnapshotId+1. 
		//When this is done, if more snpashots are missing, simply schedule another update session
		preserveNodeData.put(msg.getSource(), new PreserveData(treePath, msg.getSnapshotId(), msg.getAuthenticator(), ourSnapshotId+1));
		host.send(msg.getSource(), new UpdateBeginMessage(ourSnapshotId+1));
	}
	
	//runs on remote node
	private void onUpdateData(UpdateDataMessage msg, Node source) throws IOException {
		long currentSnapshotId = preserveNodeData.get(source).currentSnapshotId;
		String remoteTreeName = preserveNodeData.get(source).treePath + catalogTreeFile;
		String filename = SnapshotExtractorFunctions.fileNameOfExtract(remoteTreeName, currentSnapshotId);
		binaryUpdateBlock(filename, msg.getBlockNumber(), msg.getBlockData());
		host.send(source, new UpdateGetNextMessage(currentSnapshotId, msg.getBlockNumber()+1));
	}
	
	private void binaryUpdateBlock(String filename, long blockNumber, byte[] blockData) throws IOException {
		try (RandomAccessFile out = new RandomAccessFile(filename, "rw"); ) {
			out.seek(blockNumber * CHUNK_SIZE);
			out.write(blockData);
		}
	}
	
	//runs on remote node
	private void onUpdateEndOfData(UpdateEndOfDataMessage msg, Node source) throws Exception {
		String treePath = preserveNodeData.get(source).treePath;
		long targetSnapshotId = preserveNodeData.get(source).targetSnapshotId;
		byte[] targetAuthenticator = preserveNodeData.get(source).authenticator;
		Tree remoteTree = openCatalog(treePath);
		long currentSnapshotId = preserveNodeData.get(source).currentSnapshotId;
		SnapshotExtractorFunctions.apply(remoteTree, SnapshotExtractorFunctions.fileNameOfExtract(treePath + catalogTreeFile, currentSnapshotId));
		remoteTree.closeSnapshot();
		long ourNewSnapshotId = remoteTree.getLastClosedSnapshotId();
		remoteTree.close();
		preserveNodeData.remove(source);
		//If we are still behind, schedule another step to catch up
		if( currentSnapshotId < targetSnapshotId )
			host.schedule(new RemoteCatalogSyncMessage(source, targetSnapshotId, targetAuthenticator));
	}
	
	//runs on local node
	private void onUpdateBegin(UpdateBeginMessage msg, Node source) throws IOException {
		TreeExtractBlockInfo b;
		//for our local catalog, make sure specific snapshot extract is available and start breaking it to pieces and transmitting it over
		String treePath = host.getLocalCatalogPath();
		String treeFileName = treePath + catalogTreeFile;
		String filename = SnapshotExtractorFunctions.fileNameOfExtract(treeFileName, msg.getSnapshotId());
		if (! new File(filename).exists()) 
			localTree.extractSnapshot(treeFileName, msg.getSnapshotId());
		b = getChunk(filename, 0L);
		host.send(source, new UpdateDataMessage(msg.getSnapshotId(), b.number, b.data));
	}
	
	//runs on local node
	private void onUpdateGetNext(UpdateGetNextMessage msg, Node source) throws IOException {
		TreeExtractBlockInfo b;
		b = getNextBlockOfSnapshot(msg.getSnapshotId(), msg.getBlockNumber());
		if( b == null )
			host.send(source, new UpdateEndOfDataMessage(msg.getSnapshotId()));
		else
			host.send(source, new UpdateDataMessage(msg.getSnapshotId(), b.number, b.data));
	}
	
	private TreeExtractBlockInfo getNextBlockOfSnapshot(long snapshotId, long blockNumber) throws IOException {
		String treeFileName = host.getLocalCatalogPath() + catalogTreeFile;
		String filename = SnapshotExtractorFunctions.fileNameOfExtract(treeFileName, snapshotId);
		return getChunk(filename, blockNumber);
	}
	
	private static final int CHUNK_SIZE = 65536;
	private TreeExtractBlockInfo getChunk(String filename, long chunk) throws IOException {
		try (FileInputStream fis = new FileInputStream(filename)) {
			fis.skip(chunk * CHUNK_SIZE);
			final int toRead = fis.available() % CHUNK_SIZE;
			if (toRead == 0)
				return null;
			else {
				byte[] data = new byte[toRead];
				fis.read(data);
				return new TreeExtractBlockInfo(chunk, data);
			}
		}
	}
	
	/*
	 ******************************************************************************************* 
	 ************************************** Event handling *************************************
	 ******************************************************************************************* 
	 */
	
	@Override
	public void processMessage(Message msg, Node source) throws Exception {
		if( msg instanceof StoreReplyMessage ) {
			onStoreReply((StoreReplyMessage) msg, source);
		} else if( msg instanceof StoreMessage ) {
			onStore((StoreMessage) msg, source);
		} else if( msg instanceof StoredVersionReplyMessage ) {
			onStoredVersionReply((StoredVersionReplyMessage) msg, source);
		} else if( msg instanceof StoredVersionRequestMessage ) {
			onStoredVersionRequest((StoredVersionRequestMessage) msg, source);
			
		} else if( msg instanceof UpdateBeginMessage ) {
			onUpdateBegin((UpdateBeginMessage) msg, source);
		} else if( msg instanceof UpdateDataMessage ) {
			onUpdateData((UpdateDataMessage) msg, source);
		} else if( msg instanceof UpdateGetNextMessage ) {
			onUpdateGetNext((UpdateGetNextMessage) msg, source);
		} else if( msg instanceof UpdateEndOfDataMessage ) {
			onUpdateEndOfData((UpdateEndOfDataMessage) msg, source);
			
		} else if( msg instanceof RecoverVersionReplyMessage ) {
			onRecoverVersionReply((RecoverVersionReplyMessage) msg, source);
		} else if( msg instanceof RecoverVersionRequestMessage ) {
			onRecoverVersionRequest((RecoverVersionRequestMessage) msg, source);
			
		} else if( msg instanceof RecoverBeginMessage ) {
			onRecoverBegin((RecoverBeginMessage) msg, source);
		} else if( msg instanceof RecoverDataMessage ) {
			onRecoverData((RecoverDataMessage) msg, source);
		} else if( msg instanceof RecoverGetNextMessage ) {
			onRecoverGetNext((RecoverGetNextMessage) msg, source);
		} else if( msg instanceof RecoverEndOfDataMessage ) {
			onRecoverEndOfData((RecoverEndOfDataMessage) msg, source);

		} else if( msg instanceof VersionResetReplyMessage ) {
			onVersionResetReply((VersionResetReplyMessage) msg, source);
		} else if( msg instanceof VersionResetMessage ) {
			onVersionReset((VersionResetMessage) msg, source);
		
		} else if( msg instanceof RemoteCatalogSyncMessage ) {
			//source is null in this case as this message comes from the host application scheduler
			onRemoteCatalogSync((RemoteCatalogSyncMessage) msg);
		} else {
			throw new RuntimeException("Unsupported message delivered to catalog");
		}
		
	}
	
	@Override
	public void processTimeout(int timeoutId, Object extraData) throws Exception {
		switch (timeoutId) {
		case SEAL_TIMEOUT_ID:
			sealTimeoutHandler((SnapshotData) extraData);
			break;
		case VERIFY_TIMEOUT_ID:
			verifyTimeoutHandler();
			break;
		case RECOVER_PHASE1_TIMEOUT_ID:
			recoverPhase1TimeoutHandler();
			break;
		case RECOVER_PHASE2_TIMEOUT_ID:
			recoverPhase2TimeoutHandler();
			break;
		case RECOVER_PHASE3_TIMEOUT_ID:
			recoverPhase3TimeoutHandler((SnapshotData) extraData);
			break;
		default:
			throw new RuntimeException("Invalid timeout raised:" + timeoutId);
		}

	}
	
	
	/*
	 ******************************************************************************************* 
	 ************************************** Test Helpers   *************************************
	 ******************************************************************************************* 
	 */
	
	protected String debugGetBTreeFileName() {
		return host.getLocalCatalogPath() + catalogTreeFile;
	}
	
	protected String debugGetSkipListFileName() {
		return host.getLocalCatalogPath() + catalogSkipListFile;
	}
	
	protected void debugCloseTree() throws IOException {
		localTree.close();
		localTree = null;
	}
	protected void debugOpenTree() throws Exception {
		localTree = openCatalog(host.getLocalCatalogPath());		
	}

	protected void dumpRemoteTree(Node node, String filename) throws Exception {
		Tree remoteTree = openCatalog(host.getRemoteCatalogPath(node));
		remoteTree.dump(filename);
		remoteTree.close();
	}
	
	protected void dumpLocalTree(String filename) throws Exception {
		localTree.dump(filename);
	}
}

