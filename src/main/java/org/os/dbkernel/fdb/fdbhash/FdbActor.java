package org.os.dbkernel.fdb.fdbhash;

public abstract class FdbActor {

  protected FdbContext ctx;
  
  public void setup(final FdbContext ctx) {
    this.ctx = ctx;
  }
  
  public abstract ActionResult doAction(ActionParameters prms) throws Exception;

  public void cleanup() {
    ctx = null;
  }
}
