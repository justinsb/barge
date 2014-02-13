package org.robotninjas.barge;

import org.robotninjas.barge.proto.RaftEntry.Entry;

public interface ClusterConfig {

  Replica local();

  Iterable<Replica> remote();

  Replica getReplica(String info);

  void addMembershipEntry(long index, Entry entry);

}
