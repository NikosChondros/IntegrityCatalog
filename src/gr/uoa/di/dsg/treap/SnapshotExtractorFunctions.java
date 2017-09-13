package gr.uoa.di.dsg.treap;

import gr.uoa.di.dsg.util.MemoryBuffer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class SnapshotExtractorFunctions {

	public static void apply(Tree tree, String filename) throws Exception {
		try (FileInputStream fis = new FileInputStream(filename)) {
			byte[] actionArray = new byte[1];
			byte[] lenArray = new byte[4];
			MemoryBuffer mbLen = new MemoryBuffer(lenArray);

			while (fis.available() > 0) {
				fis.read(actionArray);
				fis.read(lenArray);
				byte[] key = new byte[mbLen.getInt(0)];
				fis.read(key);
				fis.read(lenArray);
				byte[] value = new byte[mbLen.getInt(0)];
				fis.read(value);
				
				switch (actionArray[0]) {
				case 1: //append
					tree.insert(key, value);
					break;
				default:
					tree.update(key, value);
				}
			}
		} catch (Exception e) {
			throw e;
		}
	}
	
	public static class SnapshotExtractorState {
		byte[] actionArray = new byte[1];
		MemoryBuffer mbAction = new MemoryBuffer(actionArray);
		byte[] lenArray = new byte[4];
		MemoryBuffer mbLen = new MemoryBuffer(lenArray);
		
		FileOutputStream fos;
		public SnapshotExtractorState(String filename) throws IOException {
			File f = new File(filename);
			if (f.exists() )
				f.delete();
			f.createNewFile();
			fos = new FileOutputStream(f);						
		}
		
		public void discard() throws IOException {
			fos.close();
		}
	}
	
	public static void writeNodeChangesToFile(NodeChanges nc, SnapshotExtractorState state) throws IOException {
		//append it to file
		state.mbAction.putByte(0, nc.isAppend ? (byte) 1 : (byte) 0);
		state.fos.write(state.actionArray);
		state.mbLen.putInt(0, nc.key.length);
		state.fos.write(state.lenArray);
		state.fos.write(nc.key);
		state.mbLen.putInt(0, nc.value.length);
		state.fos.write(state.lenArray);
		state.fos.write(nc.value);
		
	}
	
	public static String fileNameOfExtract(String treeFileName, long snapshotId) {
		return String.format("%s.SD%d.tmp", treeFileName, snapshotId);
	}
}
