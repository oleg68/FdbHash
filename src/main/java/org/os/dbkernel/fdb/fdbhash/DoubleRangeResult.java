package org.os.dbkernel.fdb.fdbhash;

public interface DoubleRangeResult extends RangeResult {

  void processKvAux(final byte[] key, final byte[] value);
  
}
