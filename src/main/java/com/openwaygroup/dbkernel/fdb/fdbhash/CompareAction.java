package com.openwaygroup.dbkernel.fdb.fdbhash;

public class CompareAction extends DoubleRangeAction {
  
  public void init(RangeAction parent, int nRange, byte[] keyFrom, byte[] keyTo) {
    res = new CompareResult();
    res.init(keyFrom, keyTo);
    super.init(nRange, res);
  }

}
