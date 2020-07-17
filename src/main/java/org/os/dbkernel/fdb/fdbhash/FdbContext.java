package org.os.dbkernel.fdb.fdbhash;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;

public class FdbContext implements AutoCloseable {
  public final Database db;
  
  public FdbContext(final FdbConnectionParameters connPrms) {
    FDB fdb = FDB.selectAPIVersion(620);
    db = fdb.open(connPrms.clusterFile);
  }
  
  @Override
  public void close() {
    db.close();
  }
}
