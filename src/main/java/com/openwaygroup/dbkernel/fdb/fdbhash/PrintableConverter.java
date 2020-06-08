package com.openwaygroup.dbkernel.fdb.fdbhash;

import java.io.ByteArrayOutputStream;

import com.apple.foundationdb.tuple.ByteArrayUtil;

public class PrintableConverter {
  
  public static boolean isPrintableAscii(char ch) {
    return
      ch >= '(' && ch <= '~' && ch != '\\';
  }
  
  public static String byteToHex(byte b) {
    final char[] hexDigits = new char[2];
    
    hexDigits[0] = Character.forDigit((b >> 4) & 0xF, 16);
    hexDigits[1] = Character.forDigit((b & 0xF), 16);
    return String.valueOf(hexDigits);
  }  
  
  public static String bytesToString(final byte[] bytes) {
    return ByteArrayUtil.printable(bytes);
  }
  
  public static byte[] stringToBytes(final String str) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final int n = str.length();
    int pos = 0;
    
    while (pos < n) {
      final char ch = str.charAt(pos ++);
      
      if (ch == '\\') {
	if (pos < n) {
	  final char ch2 = str.charAt(pos ++);
	  
	  if (ch2 == 'x' && pos + 2 <= n) {
	    baos.write(
	      (Character.digit(str.charAt(pos++), 16) << 4) | Character.digit(str.charAt(pos ++), 16)
	    );
	  } else {
	    baos.write(ch2);
	  }
	} else {
	  baos.write('\\');
	}
      } else {
	final int be = ch & 0xff00;

	baos.write(ch & 0xff);
	if (be != 0) {
	  baos.write(be >> 8);
	}
      }
    }
    return baos.toByteArray();
  }
  
}
