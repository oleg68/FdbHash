package com.openwaygroup.dbkernel.fdb.fdbhash;

import java.util.*;
import java.util.concurrent.*;

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.*;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class FdbRangeActor extends FdbActor {
  
  final RangeAction act;

  protected FdbRangeActor(RangeAction act) {
    this.act = act;
  }
  
  @Override
  public ActionResult doAction(final ActionParameters prms) throws Exception {
    act.init(null, 0, prms.getKeyFrom(), prms.getKeyTo());
    
    final ExecutorService exec = Executors.newFixedThreadPool(prms.getThreads());
    final int nQueriesToWait = prms.getMaxQueries() - 1;
    byte[] lastKey = prms.getKeyFrom();
    final List<RangeAction> subTests = new LinkedList<>();

    final Iterator<byte[]> boundaryKeyI = getRangeIterator(prms);
    boolean hasNext;

    do {
      if (prms.debug) {
	act.log("querying hasNext");
      }

      hasNext = boundaryKeyI.hasNext();

      if (prms.debug) {
	act.log("queried hasNext");
      }

      final byte[] keyFrom = lastKey;
      final byte[] keyTo = hasNext ? boundaryKeyI.next() : prms.getKeyTo();

      act.nRange ++;
      if (prms.debug) {
	act.log("waiting");
      }
      waitUntilGreather(prms, subTests, nQueriesToWait);

      if (prms.debug) {
	act.log("waited");
      }
      try {
	final RangeAction sub = act.createSubrangeTest(act.nRange, keyFrom, keyTo);

	if (prms.debug) {
	  act.log("launching");
	}
	sub.future = launchSubtest(prms, exec, keyFrom, keyTo, sub);
	if (prms.debug) {
	  act.log("launched");
	}
	subTests.add(sub);
      } catch (ReflectiveOperationException ex) {
	throw new RuntimeException(ex);
      }
      lastKey = keyTo;
    } while (hasNext);
    try {
      waitUntilGreather(prms, subTests, 0);
      exec.shutdownNow();
    } catch (Throwable th) {
      exec.shutdownNow();
      throw th;
    }
    return act.res;
  }
  
  protected Iterator<byte[]> getRangeIterator(final ActionParameters prms) {
    return getBoundaryIterator(prms, ctx.db, "db");
  }
  
  protected Iterator<byte[]> getBoundaryIterator(
    final ActionParameters prms,
    final Database db,
    final String logPrefix
  ) {
    if (prms.debug) {
      act.log("{0}: querying getBoundaryKeys", logPrefix);
    }
    try (final CloseableAsyncIterator<byte[]> boundaryKeys = LocalityUtil.getBoundaryKeys(db, prms.getKeyFrom(), prms.getKeyTo())) {
      final CompletableFuture<List<byte[]>> boundaryKeyListF = AsyncUtil.collectRemaining(boundaryKeys);

      if (prms.debug) {
	act.log("{0}: waiting getBoundaryKeys", logPrefix);
      }
      final Iterator<byte[]> boundaryKeyI = boundaryKeyListF.get().iterator();
      if (prms.debug) {
	act.log("{0}: received getBoundaryKeys", logPrefix);
      }
      if (prms.isVerbose()) {
	act.log("{0}: boundaryKeys returned", logPrefix);
      }
      return boundaryKeyI;
    } catch (InterruptedException | ExecutionException ex) {
      throw new RuntimeException(ex);
    }
  }
  
  protected CompletableFuture<Void> launchSubtest(
    final ActionParameters prms, final ExecutorService exec,
    final byte[] keyFrom, final byte[] keyTo,
    final RangeAction sub
  ) {
    return launchProcessRange(ctx.db, prms, exec, keyFrom, keyTo, sub::processKv);
  }
  
  protected static CompletableFuture<Void> launchProcessRange(
    final Database db,
    final ActionParameters prms, final ExecutorService exec,
    final byte[] keyFrom, final byte[] keyTo,
    Consumer<? super KeyValue> consumer
  ) {
    final AtomicReference<byte[]> seen = new AtomicReference<>(null);    
    final CompletableFuture<Void> future = db.readAsync(
      (ReadTransaction tr) -> {
	setTrnOptions(prms, tr);
	
	final byte[] seenKey = seen.get();
	final byte[] startKey = seenKey == null ? keyFrom : keyAfter(seenKey);
	final AsyncIterable<KeyValue> kvsi = tr.getRange(startKey, keyTo);
	
	return AsyncUtil.forEach(
	  kvsi,
	  (kv) -> {
	    seen.set(kv.getKey());
	    consumer.accept(kv);
	  }, exec
	);
      }, exec
    );
    
    return future;
  }
  
  // this method is added to ByteArrayUtil since 7.0+
  public static byte[] keyAfter(byte[] key) {
    final byte[] copy = new byte[key.length + 1];
    
    System.arraycopy(key, 0, copy, 0, key.length);
    copy[key.length] = 0x0;
    return copy;
  }


  protected static void setTrnOptions(final ActionParameters prms, ReadTransaction tr) {
    final TransactionOptions opts = tr.options();
    final boolean locked = prms.isLocked();
    final boolean system = prms.isSystem();
    final int retries = prms.getRetries();

    if (locked) {
      opts.setReadLockAware();
    }
    if (system) {
      opts.setReadSystemKeys();
    }
    if (retries > 0) {
      opts.setRetryLimit(retries);
    }
  }
  
  private void waitUntilGreather(
    final ActionParameters prms, List<RangeAction> subActs, int limit
  ) {
    while (subActs.size() > limit) {
      final RangeAction sub = subActs.remove(0);
      
      if (prms.isVerbose()) {
	act.log("waiting for {0}", sub.nRange);
      }
      try {
	if (sub.future != null) {
	  sub.future.get();
	}
      } catch (InterruptedException | ExecutionException ex) {
	throw new RuntimeException(ex);
      }
      
      final String subHash = prms.isSubres() ? sub.res.toString() : "";
      
      if (prms.isVerbose()) {
	act.log("got result of {0} {1}", sub.nRange, subHash);
      } else if (! subHash.isEmpty()) {
	System.out.println(subHash);
      }
      act.mergeSubResult(sub);
    }
  }
 
}
