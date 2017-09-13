package gr.uoa.di.dsg.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import edu.stanford.identiscape.util.ByteArrayRegion;

public class MemoryBuffer {
	protected ArrayList<ByteArrayRegion> regions = new ArrayList<>();
	
	public MemoryBuffer(byte[] data) {
		regions.add(new ByteArrayRegion(data));
	}
	
	public void append(byte[] data) {
		regions.add(new ByteArrayRegion(data));
	}

	public int size() {
		int ret = 0;
		for( ByteArrayRegion region : regions ) {
			ret += region.length;
		}
		return ret;
	}
	
	public int regionCount() {
		return regions.size();
	}
	
	public ByteArrayRegion extract(int pos, int size) {
		int currentStart = 0;
		for( ByteArrayRegion region : regions ) {
			if ( pos < currentStart + region.length ) {
				int accessStart = region.start + pos - currentStart;
				return new ByteArrayRegion(region.buffer, accessStart, size);
			} else {
				currentStart += region.length;
			}
		}
		throw new RuntimeException(String.format("extract failed to locate position %s", pos));
	}
	
	/**
	 * Assumes pos is within bounds and size is contained in single region
	 * @param pos
	 * @param size
	 */
	public void delete(int pos, int size) {
		int currentStart = 0;
		int i = 0;
		for( ByteArrayRegion region : regions ) {
			if ( pos < currentStart + region.length ) {
				//left = [region.start, pos - currentStart)
				//right = [accessStart + size, region.size - size)
				int leftStart = region.start;
				int leftSize = pos - currentStart;
				int rightStart = region.start + pos - currentStart + size;
				int rightSize = region.length - size - leftSize;
				regions.remove(i);
				if( leftSize > 0 ) {
					regions.add(i, new ByteArrayRegion(region.buffer, leftStart, leftSize));
					i++;
				}
				if( rightSize > 0 )
					regions.add(i, new ByteArrayRegion(region.buffer, rightStart, rightSize));
				return;
			} else {
				currentStart += region.length;
				i++;
			}
		}
		throw new RuntimeException(String.format("delete failed to locate position %s", pos));
	}

	/**
	 * Assumes pos falls inside the bounds and size is contained in a single region
	 * @param pos
	 * @param size
	 * @param replacement
	 */
	public void replace(int pos, int size, byte[] replacement) {
		int currentStart = 0;
		int i = 0;
		for( ByteArrayRegion region : regions ) {
			if ( pos < currentStart + region.length ) {
				//left = [region.start, pos - currentStart)
				//right = [accessStart + size, region.size - size)
				int leftStart = region.start;
				int leftSize = pos - currentStart;
				int rightStart = region.start + pos - currentStart + size;
				int rightSize = region.length - size - leftSize;
				regions.remove(i);
				if( leftSize > 0 ) {
					regions.add(i, new ByteArrayRegion(region.buffer, leftStart, leftSize));
					i++;
				}
				regions.add(i, new ByteArrayRegion(replacement));
				i++;
				if( rightSize > 0 )
					regions.add(i, new ByteArrayRegion(region.buffer, rightStart, rightSize));
				return;
			} else {
				currentStart += region.length;
				i++;
			}
		}
		throw new RuntimeException(String.format("delete failed to locate position %s", pos));
	}
	
	/**
	 * Allows 'append' by defining size() as pos
	 * @param pos
	 * @param insertedData
	 */
	public void insert(int pos, byte[] insertedData) {
		int currentStart = 0;
		int i = 0;
		for( ByteArrayRegion region : regions ) {
			if ( pos < currentStart + region.length ) {
				//left = [region.start, pos - currentStart)
				//right = [accessStart + size, region.size - size)
				int leftStart = region.start;
				int leftSize = pos - currentStart;
				int rightStart = region.start + pos - currentStart;
				int rightSize = region.length - leftSize;
				regions.remove(i);
				if( leftSize > 0 ) {
					regions.add(i, new ByteArrayRegion(region.buffer, leftStart, leftSize));
					i++;
				}
				regions.add(i, new ByteArrayRegion(insertedData));
				i++;
				if( rightSize > 0 )
					regions.add(i, new ByteArrayRegion(region.buffer, rightStart, rightSize));
				return;
			} else {
				currentStart += region.length;
				i++;
			}
		}
		if( pos == currentStart ) // in case insertion was requested immediately after the last byte
			regions.add(i, new ByteArrayRegion(insertedData));
		else
			throw new RuntimeException(String.format("insert failed to locate position %s as current length is %d", pos, currentStart));
	}
	
	public void write(byte[] output) {
		int outputPos = 0;
		for( ByteArrayRegion region : regions ) {
			System.arraycopy(region.buffer, region.start, output, outputPos, region.length);
			outputPos += region.length;
		}
	}
	
	public void write(ByteBuffer output) {
		for( ByteArrayRegion region : regions )
			output.put(region.buffer, region.start, region.length);
	}
	
	public void putUShort(int pos, int value) {
		int currentStart = 0;
		for( ByteArrayRegion region : regions ) {
			if ( pos < currentStart + region.length ) {
				int accessStart = region.start + pos - currentStart;
				region.buffer[accessStart + 0] = (byte) ((value >> 8) & 0xFF);
				region.buffer[accessStart + 1] = (byte) (value & 0xFF);
				return;
			} else {
				currentStart += region.length;
			}
		}
		throw new RuntimeException(String.format("putUShort failed to locate position %s", pos));
	}
	
	public int getUShort(int pos) {
		int currentStart = 0;
		for( ByteArrayRegion region : regions ) {
			if ( pos < currentStart + region.length ) {
				int accessStart = region.start + pos - currentStart;
				int ret = 	(((int)(region.buffer[accessStart + 0] & 0xFF)) << 8) +
							((int)(region.buffer[accessStart + 1] & 0xFF));
				return ret & 0xFFFF;  
			} else {
				currentStart += region.length;
			}
		}
		throw new RuntimeException(String.format("getUShort failed to locate position %s", pos));
	}
	
	public void putInt(int pos, int value) {
		int currentStart = 0;
		for( ByteArrayRegion region : regions ) {
			if ( pos < currentStart + region.length ) {
				int accessStart = region.start + pos - currentStart;
				region.buffer[accessStart + 0] = (byte) ((value >>> 24) & 0xFF);
				region.buffer[accessStart + 1] = (byte) ((value >>> 16) & 0xFF);
				region.buffer[accessStart + 2] = (byte) ((value >>> 8) & 0xFF);
				region.buffer[accessStart + 3] = (byte) (value & 0xFF);
				return;
			} else {
				currentStart += region.length;
			}
		}
		throw new RuntimeException(String.format("putInt failed to locate position %s", pos));
	}
	
	public int getInt(int pos) {
		int currentStart = 0;
		for( ByteArrayRegion region : regions ) {
			if ( pos < currentStart + region.length ) {
				int accessStart = region.start + pos - currentStart;
				return 	(((int)(region.buffer[accessStart + 0] & 0xFF )) << 24) +
						(((int)(region.buffer[accessStart + 1] & 0xFF )) << 16) +
						(((int)(region.buffer[accessStart + 2] & 0xFF )) << 8) +
						((int)(region.buffer[accessStart + 3] & 0xFF));
			} else {
				currentStart += region.length;
			}
		}
		throw new RuntimeException(String.format("getInt failed to locate position %s", pos));
	}
	
	public void putLong(int pos, long value) {
		int currentStart = 0;
		for( ByteArrayRegion region : regions ) {
			if ( pos < currentStart + region.length ) {
				int accessStart = region.start + pos - currentStart;
				region.buffer[accessStart + 0] = (byte) ((value >>> 56) & 0xFF);
				region.buffer[accessStart + 1] = (byte) ((value >>> 48) & 0xFF);
				region.buffer[accessStart + 2] = (byte) ((value >>> 40) & 0xFF);
				region.buffer[accessStart + 3] = (byte) ((value >>> 32) & 0xFF);
				region.buffer[accessStart + 4] = (byte) ((value >>> 24) & 0xFF);
				region.buffer[accessStart + 5] = (byte) ((value >>> 16) & 0xFF);
				region.buffer[accessStart + 6] = (byte) ((value >>> 8) & 0xFF);
				region.buffer[accessStart + 7] = (byte) (value & 0xFF);
				return;
			} else {
				currentStart += region.length;
			}
		}
		throw new RuntimeException(String.format("putLong failed to locate position %s", pos));
	}
	
	public long getLong(int pos) {
		int currentStart = 0;
		for( ByteArrayRegion region : regions ) {
			if ( pos < currentStart + region.length ) {
				int accessStart = region.start + pos - currentStart;
				return 	(((long)(region.buffer[accessStart + 0] & 0xFF )) << 56) +
						(((long)(region.buffer[accessStart + 1] & 0xFF )) << 48) +
						(((long)(region.buffer[accessStart + 2] & 0xFF )) << 40) +
						(((long)(region.buffer[accessStart + 3] & 0xFF )) << 32) +
						(((long)(region.buffer[accessStart + 4] & 0xFF )) << 24) +
						(((long)(region.buffer[accessStart + 5] & 0xFF )) << 16) +
						(((long)(region.buffer[accessStart + 6] & 0xFF )) << 8) +
						((long)(region.buffer[accessStart + 7] & 0xFF));
			} else {
				currentStart += region.length;
			}
		}
		throw new RuntimeException(String.format("getLong failed to locate position %s", pos));
	}
	
	public void putByte(int pos, byte value) {
		int currentStart = 0;
		for( ByteArrayRegion region : regions ) {
			if ( pos < currentStart + region.length ) {
				int accessStart = region.start + pos - currentStart;
				region.buffer[accessStart] = value;
				return;
			} else {
				currentStart += region.length;
			}
		}
		throw new RuntimeException(String.format("putByte failed to locate position %s", pos));
	}
	
	public byte getByte(int pos) {
		int currentStart = 0;
		for( ByteArrayRegion region : regions ) {
			if ( pos < currentStart + region.length ) {
				int accessStart = region.start + pos - currentStart;
				return region.buffer[accessStart];
			} else {
				currentStart += region.length;
			}
		}
		throw new RuntimeException(String.format("getByte failed to locate position %s", pos));
	}
	
	public void put(int pos, ByteArrayRegion values) {
		int currentStart = 0;
		for( ByteArrayRegion region : regions ) {
			if ( pos < currentStart + region.length ) {
				int accessStart = region.start + pos - currentStart;
				System.arraycopy(values.buffer,  values.start, region.buffer, accessStart, values.length);
				return;
			} else {
				currentStart += region.length;
			}
		}
		throw new RuntimeException(String.format("put failed to locate position %s", pos));
	}
	
	public void put(int pos, byte[] values) {
		int currentStart = 0;
		for( ByteArrayRegion region : regions ) {
			if ( pos < currentStart + region.length ) {
				int accessStart = region.start + pos - currentStart;
				System.arraycopy(values,  0, region.buffer, accessStart, values.length);
				return;
			} else {
				currentStart += region.length;
			}
		}
		throw new RuntimeException(String.format("put failed to locate position %s", pos));
	}
	
	public void get(int pos, byte[] values) {
		int currentStart = 0;
		for( ByteArrayRegion region : regions ) {
			if ( pos < currentStart + region.length ) {
				int accessStart = region.start + pos - currentStart;
				System.arraycopy(region.buffer, accessStart, values,  0, values.length);
				return;
			} else {
				currentStart += region.length;
			}
		}
		throw new RuntimeException(String.format("get failed to locate position %s", pos));
	}
	
	/*
	private static final short int2ushort(int v) {
		return (short) (v & 0xffff);
	}
	
	private static final int ushort2int(short v) {
		return (int) v & 0xFFFF;
	}
	*/

	public int compare(int firstStart, int firstLength, byte[] secondData, int secondStart, int secondLength) {
		int currentStart = 0;
		for( ByteArrayRegion region : regions ) {
			if ( firstStart < currentStart + region.length ) {
				int accessStart = region.start + firstStart - currentStart;
				return compare(region.buffer, accessStart, firstLength, secondData, secondStart, secondLength);
			} else {
				currentStart += region.length;
			}
		}
		throw new RuntimeException(String.format("put failed to locate position %s", firstStart));
	}
	
	public static int compare(byte[] firstData, int firstStart, int firstLength, byte[] secondData, int secondStart, int secondLength) {
		int ret, minLength;
		if( firstLength == secondLength ) {
			ret = 0;
			minLength = firstLength;
		} else if (firstLength < secondLength ) {
			ret = -1;
			minLength = firstLength;
		} else {
			ret = 1;
			minLength = secondLength;
		}
		
		for (int i = 0; i < minLength; i++)
			if (firstData[firstStart + i] < secondData[secondStart + i])
				return -1; // it's less
			else if (firstData[firstStart + i] > secondData[secondStart + i])
				return 1; // it's greater
		return ret; // all were equal so far, return result based on length
	}
}
