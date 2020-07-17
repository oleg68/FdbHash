package org.os.dbkernel.fdb.fdbhash;

import java.text.MessageFormat;
import java.util.concurrent.CompletableFuture;

import com.apple.foundationdb.KeyValue;

public abstract class RangeAction implements KvProcessor {
  protected RangeResult res;
  int nRange;
  long startTs;
  CompletableFuture<Void> future;
  
  public abstract void init(RangeAction parent, final int nRange, byte[] keyFrom, byte[] keyTo);
  
  protected void init(final int nRange, RangeResult res) {
    this.nRange = nRange;
    startTs = System.currentTimeMillis();
    this.res = res;
  }
  
  public RangeResult getTestResult() {
    return res;
  }

  public void log(final String fmt, Object ... args) {
    final long cts = System.currentTimeMillis() - startTs;
    
    System.out.println(MessageFormat.format("{0} #{1} ", cts, nRange) + MessageFormat.format(fmt, args));
  }

  public final RangeAction createSubrangeTest(final int nRange, byte[] keyFrom, byte[] keyTo)
    throws ReflectiveOperationException
  {
    final RangeAction subTest = (RangeAction) getClass().getDeclaredConstructor().newInstance();
    
    subTest.init(this, nRange, keyFrom, keyTo);
    return subTest;
  }

  public void processKv(KeyValue kv) {
    res.processKv(kv.getKey(), kv.getValue());
  }

  void mergeSubResult(RangeAction sub) {
    res.mergeFrom(sub.getTestResult());
  }
}
