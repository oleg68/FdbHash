package com.openwaygroup.dbkernel.fdb.fdbhash;

public interface RangeResult extends ActionResult {
  void init(byte[] keyFrom, byte[] keyTo);
  
  void processKv(final byte[] key, final byte[] value);

  void mergeFrom(RangeResult anotherRes);
}
