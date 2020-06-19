package com.openwaygroup.dbkernel.fdb.fdbhash;

import java.text.MessageFormat;

public class CompareResult implements DoubleRangeResult {
  
  private final HashResult hashRes1 = new HashResult();
  private final HashResult hashRes2 = new HashResult();
  
  private byte[] keyFrom;
  private byte[] keyTo;

  public void init(byte[] keyFrom, byte[] keyTo) {
    this.keyFrom = keyFrom;
    this.keyTo = keyTo;
    hashRes1.init(keyFrom, keyTo);
    hashRes2.init(keyFrom, keyTo);
  }
  
  public void processKv(final byte[] key, final byte[] value) {
    hashRes1.processKv(key, value);
  }

  public void processKvAux(final byte[] key, final byte[] value) {
    hashRes2.processKv(key, value);
  }

  public void mergeFrom(RangeResult anotherRes) {
    final CompareResult adr = (CompareResult) anotherRes;
    
    hashRes1.mergeFrom(adr.hashRes1);
    hashRes2.mergeFrom(adr.hashRes2);
  }
  
  public String toString() {
    final String resStr1 = hashRes1.getResStr();
    final String resStr2 = hashRes2.getResStr();
    final String resStr 
      = resStr2.equals(resStr1) ? "OK"
	: MessageFormat.format("res1=({0}) res2=({1})", resStr1, resStr2);
    
    return RangeResult.toString(keyFrom, keyTo, resStr);
  }
}
