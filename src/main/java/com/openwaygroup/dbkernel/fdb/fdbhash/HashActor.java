package com.openwaygroup.dbkernel.fdb.fdbhash;

import com.apple.foundationdb.*;

public class HashActor extends FdbRangeActor {
  public HashActor() {
    super(new HashAction());
  }
}
