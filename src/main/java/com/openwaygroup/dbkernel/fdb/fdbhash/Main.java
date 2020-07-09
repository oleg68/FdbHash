package com.openwaygroup.dbkernel.fdb.fdbhash;

import java.io.*;
import java.util.*;

public class Main {

  final FdbConnectionParameters connPrms = new FdbConnectionParameters();
  final FdbConnectionParameters auxConnPrms = new FdbConnectionParameters();
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
      } else if (sw.equals("A")) {
	auxConnPrms.clusterFile = checkNext(auxConnPrms.clusterFile, "A");
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
      } else if (sw.equals("subres")) {
	prms.subres = true;
      } else if (sw.equals("locked")) {
	prms.locked = true;
      } else if (sw.equals("system")) {
	prms.system = true;
      } else if (sw.equals("debug")) {
	prms.debug = true;
      } else if (sw.equals("retries")) {
	prms.retries = checkNext(prms.retries, "retries");
      } else if (sw.equals("help")) {
	prms.actionType = ActionParameters.ActionType.HELP;
      } else {
	throw new IllegalArgumentException("Unknown switch " + sw);
      }
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
      if (arg.endsWith("hash")) {
	prms.actionType = ActionParameters.ActionType.HASH;
      } else if (arg.endsWith("compare")) {
	prms.actionType = ActionParameters.ActionType.COMPARE;
      } else {
	throw new IllegalArgumentException("Invalid action type: " + arg);
      }
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
  
  static void printUsage() throws IOException {
    try (InputStream is = Main.class.getResourceAsStream("/usage.txt")) {
      copyStream(is, System.out);
    }
  }

  public static void copyStream(InputStream input, OutputStream output) throws IOException {
    final int READ_BUFFER_SIZE = 4096;

    byte[] buffer = new byte[READ_BUFFER_SIZE];
    while (true) {
      int count = input.read(buffer);
      if (count == -1) {
	break;
      }
      output.write(buffer, 0, count);
    }
  }    

  void run() throws Exception {
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
      case COMPARE:
	System.out.println("ClusterFile=" + connPrms.clusterFile);
	System.out.println("auxClusterFile=" + auxConnPrms.clusterFile);
	try (
	  FdbContext ctx = new FdbContext(connPrms);
	  FdbContext auxCtx = new FdbContext(auxConnPrms)
	) {

	  final CompareActor actor = new CompareActor();

	  actor.setup(ctx, auxCtx);

	  final ActionResult res = actor.doAction(prms);

	  System.out.println(res.toString());
	  actor.cleanup();
	}
	break;
      case HELP:
	printUsage();
	break;
    }
  }
  
}
