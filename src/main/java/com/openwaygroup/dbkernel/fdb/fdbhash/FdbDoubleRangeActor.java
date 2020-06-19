package com.openwaygroup.dbkernel.fdb.fdbhash;

import com.apple.foundationdb.tuple.ByteArrayUtil;
import static com.openwaygroup.dbkernel.fdb.fdbhash.FdbRangeActor.launchProcessRange;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public abstract class FdbDoubleRangeActor extends FdbRangeActor {

  protected FdbContext auxCtx;
  
  protected FdbDoubleRangeActor(RangeAction act) {
    super(act);
  }
  
  public void setup(final FdbContext ctx, final FdbContext auxCtx) {
    super.setup(ctx);
    this.auxCtx = auxCtx;
  }
  
  protected Iterator<byte[]> getRangeIterator(final ActionParameters prms) {
    final Iterator it1 = getBoundaryIterator(prms, ctx.db, "db");
    final Iterator it2 = getBoundaryIterator(prms, auxCtx.db, "dbAux");
    
    return mergeKeysIters(it1, it2);
  }
  
  static Iterator<byte[]> mergeKeysIters(
    final Iterator<byte[]> it1,
    final Iterator<byte[]> it2
  ) {
    return IterUtils.merge(
      it1, it2, 
      (b1, b2) -> {
	return ByteArrayUtil.compareUnsigned(b1, b2);
      }, true
    );
  }
  
  @Override
  protected CompletableFuture<Void> launchSubtest(
    final ActionParameters prms, final ExecutorService exec,
    final byte[] keyFrom, final byte[] keyTo,
    final RangeAction sub
  ) {
    final DoubleRangeAction dra = (DoubleRangeAction) sub;
    
    return CompletableFuture.allOf(
      launchProcessRange(ctx.db, prms, exec, keyFrom, keyTo, dra::processKv),
      launchProcessRange(auxCtx.db, prms, exec, keyFrom, keyTo, dra::processKvAux)
    );
  }
  
  @Override
  public void cleanup() {
    this.auxCtx = null;
    super.cleanup();
  }
}
