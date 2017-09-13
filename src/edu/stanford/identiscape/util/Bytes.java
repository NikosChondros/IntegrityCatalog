/*
 * This class was taken directly from the Apach JServ and TomCat
 * sources.  Original copyright information follows.

 * Copyright (c) 1997-1999 The Java Apache Project.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. All advertising materials mentioning features or use of this
 *    software must display the following acknowledgment:
 *    "This product includes software developed by the Java Apache 
 *    Project for use in the Apache JServ servlet engine project
 *    <http://java.apache.org/>."
 *
 * 4. The names "Apache JServ", "Apache JServ Servlet Engine" and 
 *    "Java Apache Project" must not be used to endorse or promote products 
 *    derived from this software without prior written permission.
 *
 * 5. Products derived from this software may not be called "Apache JServ"
 *    nor may "Apache" nor "Apache JServ" appear in their names without 
 *    prior written permission of the Java Apache Project.
 *
 * 6. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by the Java Apache 
 *    Project for use in the Apache JServ servlet engine project
 *    <http://java.apache.org/>."
 *    
 * THIS SOFTWARE IS PROVIDED BY THE JAVA APACHE PROJECT "AS IS" AND ANY
 * EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE JAVA APACHE PROJECT OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Java Apache Group. For more information
 * on the Java Apache Project and the Apache JServ Servlet Engine project,
 * please see <http://java.apache.org/>.
 * */

package edu.stanford.identiscape.util;

import java.io.ByteArrayOutputStream;

/**
 * Static methods for managing byte arrays (all methods follow Big Endian order
 * where most significant bits are in front).
 * 
 * @author <a href="mailto:stefano@apache.org">Stefano Mazzocchi</a>
 * @version $Revision: 1.9 $ $Date: 2002/07/21 19:14:43 $
 */
public class Bytes {

	/** The local int helper */
	private static byte[] intHelper_ = new byte[4];

	/** The local long helper */
	private static byte[] longHelper_ = new byte[8];

	/**
	 * Build an int from first 4 bytes of the array.
	 * 
	 * @param b
	 *            the byte array to convert.
	 * @param o
	 *            the offset
	 */
	public static int toInt(byte[] b, int o) {
		return (((int) b[o + 3]) & 0xFF) + ((((int) b[o + 2]) & 0xFF) << 8)
				+ ((((int) b[o + 1]) & 0xFF) << 16)
				+ ((((int) b[o]) & 0xFF) << 24);
	}

	/**
	 * Build an int from first 4 bytes of the array.
	 * 
	 * @param b
	 *            the byte array to convert.
	 */
	public static int toInt(byte[] b) {
		return toInt(b, 0);
	}

	/**
	 * Build a long from first 8 bytes of the array.
	 * 
	 * @param b
	 *            the byte array to convert.
	 */
	public static long toLong(byte[] b) {
		return (((long) b[7]) & 0xFF) + ((((long) b[6]) & 0xFF) << 8)
				+ ((((long) b[5]) & 0xFF) << 16)
				+ ((((long) b[4]) & 0xFF) << 24)
				+ ((((long) b[3]) & 0xFF) << 32)
				+ ((((long) b[2]) & 0xFF) << 40)
				+ ((((long) b[1]) & 0xFF) << 48)
				+ ((((long) b[0]) & 0xFF) << 56);
	}

	/**
	 * Build a long from first 8 bytes of an array portion. No array index
	 * checks are performed.
	 * 
	 * @param b
	 *            the byte array to convert.
	 * @param s
	 *            the starting index into the array.
	 * @return the resulting long.
	 */
	public static long toLong(byte[] b, int s) {
		return (((long) b[s + 7]) & 0xFF) + ((((long) b[s + 6]) & 0xFF) << 8)
				+ ((((long) b[s + 5]) & 0xFF) << 16)
				+ ((((long) b[s + 4]) & 0xFF) << 24)
				+ ((((long) b[s + 3]) & 0xFF) << 32)
				+ ((((long) b[s + 2]) & 0xFF) << 40)
				+ ((((long) b[s + 1]) & 0xFF) << 48)
				+ ((((long) b[s]) & 0xFF) << 56);
	}

	/**
	 * Returns a 4-byte array built from an int.
	 * 
	 * @param n
	 *            the number to convert.
	 */
	public static byte[] intToBytes(int n) {
		return intToBytes(n, new byte[4], 0);
	}

	/**
	 * Returns a 4-byte array built from an int. The array will be reused, so it
	 * will only remain consistent until the next invocation of the method
	 */
	public static byte[] intToBytesInPlace(int n) {
		return intToBytes(n, intHelper_, 0);
	}

	/**
	 * Build a 4-byte array from an int. No check is performed on the array
	 * length.
	 * 
	 * @param n
	 *            the number to convert.
	 * @param b
	 *            the array to fill.
	 * @param o
	 *            the offset into the array.
	 */
	public static byte[] intToBytes(int n, byte[] b, int o) {
		b[3 + o] = (byte) (n);
		n >>>= 8;
		b[2 + o] = (byte) (n);
		n >>>= 8;
		b[1 + o] = (byte) (n);
		n >>>= 8;
		b[0 + o] = (byte) (n);

		return b;
	}

	/**
	 * Write a short into a byte array. No length check is performed.
	 * 
	 * @param bytes
	 *            The array.
	 * @param s
	 *            The short.
	 * @param start
	 *            The first byte index to use.
	 */
	public static void shortToBytes(short s, byte[] bytes, int start) {
		bytes[start + 1] = (byte) (s & 0xFF);
		s >>>= 8;
		bytes[start] = (byte) (s & 0xFF);
	}

	/**
	 * Build a short from first 2 bytes of the array.
	 * 
	 * @param b
	 *            the byte array to convert.
	 * @param start
	 *            the first byte in the array to use.
	 */
	public static short toShort(byte[] b, int start) {
		return (short) (((((short) b[start]) & 0xFF) << 8) + ((short) (((short) b[start + 1]) & 0xFF)));
	}

	/**
	 * Returns a 8-byte array built from a long.
	 * 
	 * @param n
	 *            the number to convert.
	 */
	public static byte[] longToBytes(long n) {
		return longToBytes(n, new byte[8]);
	}

	/**
	 * Returns an 8-byte array built from a long. The array will be reused, so
	 * it will only remain consistent until the next invocation of the method
	 */
	public static byte[] longToBytesInPlace(long n) {
		return longToBytes(n, longHelper_);
	}

	/**
	 * Build a 8-byte array from a long. No check is performed on the array
	 * length.
	 * 
	 * @param n
	 *            the number to convert.
	 * @param b
	 *            the array to fill.
	 */
	public static byte[] longToBytes(long n, byte[] b) {
		b[7] = (byte) (n);
		n >>>= 8;
		b[6] = (byte) (n);
		n >>>= 8;
		b[5] = (byte) (n);
		n >>>= 8;
		b[4] = (byte) (n);
		n >>>= 8;
		b[3] = (byte) (n);
		n >>>= 8;
		b[2] = (byte) (n);
		n >>>= 8;
		b[1] = (byte) (n);
		n >>>= 8;
		b[0] = (byte) (n);

		return b;
	}

	/**
	 * Build a 8-byte array from a long. No check is performed on the array
	 * length.
	 * 
	 * @param n
	 *            the number to convert.
	 * @param b
	 *            the array to fill.
	 * @param s
	 *            the starting index in the array.
	 */
	public static void longToBytes(long n, byte[] b, int s) {
		b[s + 7] = (byte) (n);
		n >>>= 8;
		b[s + 6] = (byte) (n);
		n >>>= 8;
		b[s + 5] = (byte) (n);
		n >>>= 8;
		b[s + 4] = (byte) (n);
		n >>>= 8;
		b[s + 3] = (byte) (n);
		n >>>= 8;
		b[s + 2] = (byte) (n);
		n >>>= 8;
		b[s + 1] = (byte) (n);
		n >>>= 8;
		b[s] = (byte) (n);
	}

	/**
	 * Compares two byte arrays for equality.
	 * 
	 * @return true if the arrays have identical contents
	 */
	public static boolean areEqual(byte[] a, byte[] b) {
		int aLength = a.length;
		if (aLength != b.length)
			return false;

		for (int i = 0; i < aLength; i++)
			if (a[i] != b[i])
				return false;

		return true;
	}

	/**
	 * Compares portions of two byte arrays for equality.
	 * 
	 * @return true if the arrays portions have identical contents
	 * */
	public static boolean areEqual(byte[] a, int aStart, byte[] b, int bStart,
			int size) {
		return compare(a, aStart, b, bStart, size) == 0;
	}

	/**
	 * Compare a byte array portion to another, byte by byte. Negative result
	 * means the first is less, positive result means the first is greater, 0
	 * result means they are equal. Both portions are assumed to have the same
	 * given length. No array index checks are performed.
	 */
	public static int compare(byte[] first, int firstStart, byte[] second,
			int secondStart, int length) {
		for (int i = 0; i < length; i++) {
			if (first[firstStart] < second[secondStart]) {
				return -1; // it's less
			} else if (first[firstStart] > second[secondStart]) {
				return 1; // it's greater
			}
			firstStart++;
			secondStart++;
		}
		return 0; // all were equal
	}

	/**
	 * Convenience function that transforms the second parameter to a ByteArrayRegion wrapping the complete array and calls the appropriate compare function
	 * @param first	ByteArrayRegion
	 * @param second byte[], will be wrapped to a ByteArrayRegion at its entirety
	 * @return
	 */
	public static int compare(ByteArrayRegion first, byte[] second) {
		return compare(first, new ByteArrayRegion(second));
	}
	/**
	 * Compare a byte array portion to another, byte by byte. Negative result
	 * means the first is less, positive result means the first is greater, 0
	 * result means they are equal. Portions are allowed to have different sizes.
	 * A shorter one is assumed to be "less" if otherwise equal
	 * No array index checks are performed.
	 */
	public static int compare(ByteArrayRegion first, ByteArrayRegion second) {
		int ret, minLength;
		if( first.length == second.length ) {
			ret = 0;
			minLength = first.length;
		} else if (first.length < second.length ) {
			ret = -1;
			minLength = first.length;
		} else {
			ret = 1;
			minLength = second.length;
		}
		
		for (int i = 0; i < minLength; i++) {
			if (first.buffer[first.start + i] < second.buffer[second.start + i]) {
				return -1; // it's less
			} else if (first.buffer[first.start + i] > second.buffer[second.start + i]) {
				return 1; // it's greater
			}
		}
		return ret; // all were equal so far, return result based on length
	}
	
	/**
	 * Appends two bytes array into one.
	 */
	public static byte[] append(byte[] a, byte[] b) {
		byte[] z = new byte[a.length + b.length];
		System.arraycopy(a, 0, z, 0, a.length);
		System.arraycopy(b, 0, z, a.length, b.length);
		return z;
	}

	/**
	 * Appends three bytes array into one.
	 */
	public static byte[] append(byte[] a, byte[] b, byte[] c) {
		byte[] z = new byte[a.length + b.length + c.length];
		System.arraycopy(a, 0, z, 0, a.length);
		System.arraycopy(b, 0, z, a.length, b.length);
		System.arraycopy(c, 0, z, a.length + b.length, c.length);
		return z;
	}

	/**
	 * Gets the end of the byte array given.
	 * 
	 * @param b
	 *            byte array
	 * @param pos
	 *            the position from which to start
	 * @return a byte array consisting of the portion of b between pos and the
	 *         end of b.
	 */
	public static byte[] copy(byte[] b, int pos) {
		return copy(b, pos, b.length - pos);
	}

	/**
	 * Gets a sub-set of the byte array given.
	 * 
	 * @param b
	 *            byte array
	 * @param pos
	 *            the position from which to start
	 * @param length
	 *            the number of bytes to copy from the original byte array to
	 *            the new one.
	 * @return a byte array consisting of the portion of b starting at pos and
	 *         continuing for length bytes, or until the end of b is reached,
	 *         which ever occurs first.
	 */
	public static byte[] copy(byte[] b, int pos, int length) {
		byte[] z = new byte[length];
		System.arraycopy(b, pos, z, 0, length);
		return z;
	}

	/**
	 * Merges a bytes array into another starting from the given positions.
	 */
	public static void merge(byte[] src, byte[] dest, int srcpos, int destpos,
			int length) {
		System.arraycopy(src, srcpos, dest, destpos, length);
	}

	/**
	 * Merges a bytes array into another starting from the given position.
	 */
	public static void merge(byte[] src, byte[] dest, int pos) {
		System.arraycopy(src, 0, dest, pos, src.length);
	}

	/**
	 * Merges a bytes array into another.
	 */
	public static void merge(byte[] src, byte[] dest) {
		System.arraycopy(src, 0, dest, 0, src.length);
	}

	/**
	 * Merges a bytes array into another starting from the given position.
	 */
	public static void merge(byte[] src, byte[] dest, int pos, int length) {
		System.arraycopy(src, 0, dest, pos, length);
	}

	private static final char[] hexDigits = { '0', '1', '2', '3', '4', '5',
			'6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

	/**
	 * Returns a string of hexadecimal digits from a byte array, starting at
	 * offset and continuing for length bytes.
	 */
	public static String toString(byte[] b, int offset, int length) {
		char[] buf = new char[length * 2];

		for (int i = offset, j = 0, k; i < offset + length; i++) {
			k = b[i];
			buf[j++] = hexDigits[(k >>> 4) & 0x0F];
			buf[j++] = hexDigits[k & 0x0F];
		}

		return new String(buf);
	}

	/**
	 * Returns a string of hexadecimal digits from a byte array..
	 */
	public static String toString(byte[] b) {
		return toString(b, 0, b.length);
	}

	/** Returns a byte array from a hexadecimal string */
	public static byte[] stringToBytes(String string) {
		return HexUtils.convert(string);
	}

	/*
	 * $Header:
	 * /shareddata/CVS/maniatis/IdentiScape/tangles/sourcepath/edu/stanford
	 * /identiscape/util/Bytes.java,v 1.9 2002/07/21 19:14:43 maniatis Exp $
	 * $Revision: 1.9 $ $Date: 2002/07/21 19:14:43 $
	 * 
	 * ====================================================================
	 * 
	 * The Apache Software License, Version 1.1
	 * 
	 * Copyright (c) 1999 The Apache Software Foundation. All rights reserved.
	 * 
	 * Redistribution and use in source and binary forms, with or without
	 * modification, are permitted provided that the following conditions are
	 * met:
	 * 
	 * 1. Redistributions of source code must retain the above copyright notice,
	 * this list of conditions and the following disclaimer.
	 * 
	 * 2. Redistributions in binary form must reproduce the above copyright
	 * notice, this list of conditions and the following disclaimer in the
	 * documentation and/or other materials provided with the distribution.
	 * 
	 * 3. The end-user documentation included with the redistribution, if any,
	 * must include the following acknowlegement: "This product includes
	 * software developed by the Apache Software Foundation
	 * (http://www.apache.org/)." Alternately, this acknowlegement may appear in
	 * the software itself, if and wherever such third-party acknowlegements
	 * normally appear.
	 * 
	 * 4. The names "The Jakarta Project", "Tomcat", and "Apache Software
	 * Foundation" must not be used to endorse or promote products derived from
	 * this software without prior written permission. For written permission,
	 * please contact apache@apache.org.
	 * 
	 * 5. Products derived from this software may not be called "Apache" nor may
	 * "Apache" appear in their names without prior written permission of the
	 * Apache Group.
	 * 
	 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
	 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
	 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN
	 * NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR ITS CONTRIBUTORS BE
	 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
	 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
	 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
	 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
	 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
	 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
	 * THE POSSIBILITY OF SUCH DAMAGE.
	 * ====================================================================
	 * 
	 * This software consists of voluntary contributions made by many
	 * individuals on behalf of the Apache Software Foundation. For more
	 * information on the Apache Software Foundation, please see
	 * <http://www.apache.org/>.
	 * 
	 * [Additional notices, if required by prior licensing conditions]
	 */

	/**
	 * Library of utility methods useful in dealing with converting byte arrays
	 * to and from strings of hexadecimal digits.
	 * 
	 * @author Craig R. McClanahan
	 */
	private static final class HexUtils {
		// Code from Ajp11, from Apache's JServ

		// Table for HEX to DEC byte translation
		public static final int[] DEC = { -1, -1, -1, -1, -1, -1, -1, -1, -1,
				-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
				-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
				-1, -1, -1, -1, -1, -1, -1, 00, 01, 02, 03, 04, 05, 06, 07, 8,
				9, -1, -1, -1, -1, -1, -1, -1, 10, 11, 12, 13, 14, 15, -1, -1,
				-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
				-1, -1, -1, -1, -1, -1, -1, -1, 10, 11, 12, 13, 14, 15, -1, -1,
				-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
				-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
				-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
				-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
				-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
				-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
				-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
				-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
				-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
				-1, -1, -1, -1, -1, -1, -1, };

		/**
		 * Convert a String of hexadecimal digits into the corresponding byte
		 * array by encoding each two hexadecimal digits as a byte.
		 * 
		 * @param digits
		 *            Hexadecimal digits representation
		 * 
		 * @exception IllegalArgumentException
		 *                if an invalid hexadecimal digit is found, or the input
		 *                string contains an odd number of hexadecimal digits
		 */
		public static byte[] convert(String digits) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			for (int i = 0; i < digits.length(); i += 2) {
				char c1 = digits.charAt(i);
				if ((i + 1) >= digits.length()) {
					throw new IllegalArgumentException("Odd number "
							+ " of hex " + "digits");
				}
				char c2 = digits.charAt(i + 1);
				byte b = 0;
				if ((c1 >= '0') && (c1 <= '9')) {
					b += ((c1 - '0') * 16);
				} else if ((c1 >= 'a') && (c1 <= 'f')) {
					b += ((c1 - 'a' + 10) * 16);
				} else if ((c1 >= 'A') && (c1 <= 'F')) {
					b += ((c1 - 'A' + 10) * 16);
				} else {
					throw new IllegalArgumentException("Bad chars");
				}
				if ((c2 >= '0') && (c2 <= '9')) {
					b += (c2 - '0');
				} else if ((c2 >= 'a') && (c2 <= 'f')) {
					b += (c2 - 'a' + 10);
				} else if ((c2 >= 'A') && (c2 <= 'F')) {
					b += (c2 - 'A' + 10);
				} else {
					throw new IllegalArgumentException("Bad chars");
				}
				baos.write(b);
			}
			return (baos.toByteArray());
		}
	}
}
