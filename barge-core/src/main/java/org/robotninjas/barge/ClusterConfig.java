package org.robotninjas.barge;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class ClusterConfig implements Serializable {
  public static final long DEFAULT_TIMEOUT = 225;

  public final Replica self;
  public final List<Replica> allMembers;
  public final long electionTimeout;
  
  public ClusterConfig(Replica self, List<Replica> allMembers, long electionTimeout) {
    this.self = self;
    this.electionTimeout = electionTimeout;
    this.allMembers = Collections.unmodifiableList(allMembers);
  }
}
