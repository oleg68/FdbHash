package org.os.dbkernel.fdb.fdbhash;

import java.text.MessageFormat;

public interface RangeResult extends ActionResult {
  void init(byte[] keyFrom, byte[] keyTo);
  
  void processKv(final byte[] key, final byte[] value);

  void mergeFrom(RangeResult anotherRes);
  
  static String toString(byte[] keyFrom, byte[] keyTo, final String resStr) {
    return MessageFormat.format(
      "{0} {1} {2}",
      PrintableConverter.bytesToString(keyFrom),
      PrintableConverter.bytesToString(keyTo),
      resStr
    );
  }
}
