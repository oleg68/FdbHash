package org.os.dbkernel.fdb.fdbhash;

import java.util.*;

public class IterUtils {

  public static<T> Iterator<T> merge(
    final Iterator<T> it1,
    final Iterator<T> it2, 
    final Comparator<T> comp,
    final boolean distinct
  ) {
    return new Iterator<T>() {
      private T o1 = fetchOrNull(it1);
      private T o2 = fetchOrNull(it2);

      @Override
      public boolean hasNext() {
	  return o1 != null || o2 != null;
      }

      @Override
      public T next() {
	  if (o1 == null && o2 == null)
	      throw new NoSuchElementException();
	  
	  T ret;
	  
	  if (o1 == null) {
	      ret = o2;
	      o2 = fetchOrNull(it2);
	  } else if (o2 == null) {
	      ret = o1;
	      o1 = fetchOrNull(it1);
	  } else {
	    int c = comp.compare(o1, o2);
	    
	    if (c <= 0) {
	      ret = o1;
	      o1 = fetchOrNull(it1);
	      if (c == 0 && distinct) {
		o2 = fetchOrNull(it2);
	      }
	    } else {
	      ret = o2;
	      o2 = fetchOrNull(it2);
	    }
	  }
	  return ret;
      }

      @Override
      public void remove() {
	  throw new UnsupportedOperationException("Not implemented");
      }
    };
  }
  
  public static<T> T fetchOrNull(Iterator<T> iter) {
    return iter.hasNext() ? iter.next(): null;
  }
}
