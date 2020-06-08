package com.openwaygroup.dbkernel.fdb.fdbhash;

import java.util.*;

public class Main {

  final FdbConnectionParameters connPrms = new FdbConnectionParameters();
  final ActionParameters prms = new ActionParameters();
  final ParameterParser prs;
  
  class ParameterParser {
    final Iterator<String> argsIter;
    
    ParameterParser(String[] args) {
      argsIter = Arrays.asList(args).iterator();
    }

    void parseParameters() {
      while (argsIter.hasNext()) {
	final String arg = argsIter.next();

	if (arg.startsWith("-")) {
	  consumeSwitch(arg.substring(1));
	} else {
	  consumeArg(arg);
	}
      }
    }

    void consumeSwitch(final String sw) {
      if (sw.equals("C")) {
	connPrms.clusterFile = checkNext(connPrms.clusterFile, "C");
      } else if (sw.equals("from")) {
	prms.keyFrom = PrintableConverter.stringToBytes(checkNext(prms.keyFrom, "from"));
      } else if (sw.equals("to")) {
	prms.keyTo = PrintableConverter.stringToBytes(checkNext(prms.keyTo, "to"));
      } else if (sw.equals("threads")) {
	prms.threads = checkNext(prms.threads, "threads");
      } else if (sw.equals("max_queries")) {
	prms.maxQueries = checkNext(prms.maxQueries, "max_queries");
      } else if (sw.equals("v")) {
	prms.verbose = true;
      } else if (sw.equals("subhash")) {
	prms.subhash = true;
      } else if (sw.equals("locked")) {
	prms.locked = true;
      } else if (sw.equals("system")) {
	prms.system = true;
      } else if (sw.equals("retries")) {
	prms.retries = checkNext(prms.retries, "retries");
      } else if (sw.equals("help")) {
	prms.actionType = ActionParameters.ActionType.HELP;
      } else {
	throw new IllegalArgumentException("Unknown switch " + sw);
      }
    }
    
    void printUsage() {
      System.out.println("Calculating hashsum of the fdb instance");
      System.out.println("Usage: java -jar fdb-hash {options}...");
      System.out.println("Options are:");
      System.out.println("-C ClusterFile     Path to the fdb cluster file. Default: uses FDB_CLUSER_FILE environment variable");
      System.out.println("-from Key          Beginning key. Default: \\x00");
      System.out.println("-to Key            Ending key. Default: \\xff");
      System.out.println("-threads N         Number of parallel threads. Default: 10");
      System.out.println("-max_queries N     Maximum outstanding queries. Default: 30");
      System.out.println("-v                 Output progress. Default: no");
      System.out.println("-subhash           Print hash for every subinterval queried. Default: no");
      System.out.println("-locked            Query against locked database (eg dr site)");
      System.out.println("-system            Query against the system key space (\\xff - \\xff\\xff)");
      System.out.println("-subhash           Print hash for every subinterval queried. Default: no");
      System.out.println("-retries N         Limit number of retries of each query on recoverable errors to N. Default: 3");
      System.out.println("-help              Print this information");
    }
    
    String checkNext(final Object oldV, final String argName) {
      if (oldV != null) {
	throw new IllegalArgumentException("Duplicate " + argName);
      }
      if (! argsIter.hasNext()) {
	throw new IllegalArgumentException("No value for " + argName);
      }
      return argsIter.next();
    }
    
    int checkNext(final int oldV, final String argName) {
      if (oldV > 0) {
	throw new IllegalArgumentException("Duplicate " + argName);
      }
      if (! argsIter.hasNext()) {
	throw new IllegalArgumentException("No value for " + argName);
      }
      
      final int newV = Integer.valueOf(argsIter.next());
      
      if (newV <= 0) {
	throw new IllegalArgumentException("Invalid value of " + argName + ": " + newV);
      }
      return newV;
    }
    
    void consumeArg(final String arg) {
      throw new IllegalArgumentException("Invalid arg: " + arg);
    }
  }

  public static void main(String[] args) {
    try {
      new Main(args).run();
    } catch (Throwable th) {
      th.printStackTrace();
      System.exit(1);
    }
  }
  
  Main(String[] args) {
    prs = new ParameterParser(args);
    prs.parseParameters();
  }
  
  void run() {
    switch (prms.actionType) {
      case HASH:
	System.out.println("ClusterFile=" + connPrms.clusterFile);
	try (FdbContext ctx = new FdbContext(connPrms)) {

	  final FdbActor actor = new HashActor();

	  actor.setup(ctx);

	  final ActionResult res = actor.doAction(prms);

	  System.out.println(res.toString());
	  actor.cleanup();
	}
	break;
      case HELP:
	prs.printUsage();
	break;
    }
  }
  
}
