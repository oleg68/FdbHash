package org.os.dbkernel.fdb.fdbhash;

public class HashAction extends RangeAction {

  public void init(RangeAction parent, int nRange, byte[] keyFrom, byte[] keyTo) {
    res = new HashResult();
    res.init(keyFrom, keyTo);
    super.init(nRange, res);
  }
}
