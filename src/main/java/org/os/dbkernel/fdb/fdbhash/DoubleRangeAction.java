package org.os.dbkernel.fdb.fdbhash;

import com.apple.foundationdb.KeyValue;

public abstract class DoubleRangeAction extends RangeAction {
  protected void init(final int nRange, RangeResult res) {
    super.init(nRange, (DoubleRangeResult) res);
  }
  
  public void processKvAux(KeyValue kv) {
    ((DoubleRangeResult) res).processKvAux(kv.getKey(), kv.getValue());
  }
}
