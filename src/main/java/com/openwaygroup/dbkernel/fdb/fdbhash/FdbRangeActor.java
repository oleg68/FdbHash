package com.openwaygroup.dbkernel.fdb.fdbhash;

import java.util.*;
import java.util.concurrent.*;

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.*;

public class FdbRangeActor extends FdbActor {
  
  final RangeAction act;

  protected FdbRangeActor(RangeAction act) {
    this.act = act;
  }
  
  @Override
  public ActionResult doAction(final ActionParameters prms) {
    act.init(null, 0, prms.getKeyFrom(), prms.getKeyTo());
    
    final ExecutorService exec = Executors.newFixedThreadPool(prms.getThreads());
    
    final CloseableAsyncIterator<byte[]> boundaryKeys = LocalityUtil.getBoundaryKeys(ctx.db, prms.getKeyFrom(), prms.getKeyTo());
    byte[] lastKey = prms.getKeyFrom();
    final List<RangeAction> subTests = new LinkedList<>();
    final int nQueriesToWait = prms.getMaxQueries() - 1;
    boolean hasNext;
    
    act.log("boundaryKeys returned");
    do {
      hasNext = boundaryKeys.hasNext();

      final byte[] keyFrom = lastKey;
      final byte[] keyTo = hasNext ? boundaryKeys.next() : prms.getKeyTo();

      act.nRange ++;
      waitUntilGreather(subTests, nQueriesToWait);
      
      
      try {
	final RangeAction sub = act.createSubrangeTest(act.nRange, keyFrom, keyTo);

	sub.future = ctx.db.readAsync(
	  (ReadTransaction tr) -> {
	    final AsyncIterable<KeyValue> kvsi = tr.getRange(keyFrom, keyTo);
	    
	    return AsyncUtil.forEach(kvsi, sub::processKv, exec);
	  }, exec
	);
	subTests.add(sub);
      } catch (ReflectiveOperationException ex) {
	throw new RuntimeException(ex);
      }
      lastKey = keyTo;
    } while (hasNext);
    waitUntilGreather(subTests, 0);
    exec.shutdownNow();
    return act.res;
  }
  
  private void waitUntilGreather(
    List<RangeAction> subActs, int limit
  ) {
    while (subActs.size() > limit) {
      final RangeAction sub = subActs.remove(0);
      
      act.log("waiting for {0}", sub.nRange);
      try {
	if (sub.future != null) {
	  sub.future.get();
	}
      } catch (InterruptedException | ExecutionException ex) {
	throw new RuntimeException(ex);
      }
      act.mergeSubResult(sub);
      act.log("got result of {0}", sub.nRange);
    }
  }
 
}
