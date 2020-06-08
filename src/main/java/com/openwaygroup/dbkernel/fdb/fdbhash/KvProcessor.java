package com.openwaygroup.dbkernel.fdb.fdbhash;

import com.apple.foundationdb.*;

public interface KvProcessor {
  
  public void processKv(KeyValue kv);
  
}
