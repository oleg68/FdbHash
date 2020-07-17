package org.os.dbkernel.fdb.fdbhash;

public class CompareActor extends FdbDoubleRangeActor {
  public CompareActor() {
    super(new CompareAction());
  }
}
