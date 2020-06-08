package com.openwaygroup.dbkernel.fdb.fdbhash;

public class ActionParameters {

  public static final byte[] KEY_FROM_DFLT = {0};
  public static final byte[] KEY_TO_DFLT = {(byte) 0xff};
  public static final int THREADS_DFLT = 10;
  public static final int MAX_QUERIES_DFLT = 30;
  public static final int RETRIES_DFLT = 3;

  public enum ActionType {HASH, HELP};
  
  ActionType actionType = ActionType.HASH;
  byte[] keyFrom;
  byte[] keyTo;
  int threads = 0;
  int maxQueries = 0;
  boolean verbose = false;
  boolean subhash = false;
  boolean locked = false;
  boolean system = false;
  int retries = 0;
  
  public ActionType getActionType() {
    return actionType;
  }

  public byte[] getKeyFrom() {
    return keyFrom != null ? keyFrom : KEY_FROM_DFLT;
  }

  public byte[] getKeyTo() {
    return keyTo != null ? keyTo : KEY_TO_DFLT;
  }
  
  public int getThreads() {
    return threads > 0 ? threads : THREADS_DFLT;
  }

  public int getMaxQueries() {
    return maxQueries > 0 ? maxQueries : MAX_QUERIES_DFLT;
  }
  
  public boolean isVerbose() {
    return verbose;
  }
  
  public boolean isSubhash() {
    return subhash;
  }
  
  public boolean isLocked() {
    return locked;
  }

  public boolean isSystem() {
    return system;
  }
  
  public int getRetries() {
    return retries > 0 ? retries : RETRIES_DFLT;
  }
}

