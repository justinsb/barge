package org.robotninjas.barge;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.robotninjas.barge.proto.RaftEntry.ConfigTimeouts;

public class ClusterConfig implements Serializable {
  public final Replica self;
  public final List<Replica> allMembers;

  public final ConfigTimeouts timeouts;
  
  public ClusterConfig(Replica self, List<Replica> allMembers, ConfigTimeouts timeouts) {
    this.self = self;
    this.timeouts = timeouts;
    this.allMembers = Collections.unmodifiableList(allMembers);
  }

  public static ConfigTimeouts buildDefaultTimeouts() {
    ConfigTimeouts.Builder b = ConfigTimeouts.newBuilder();
    b.setHeartbeatInterval(50);
    b.setFollowerElectionStartDelay(1000);
    b.setCandidateElectionTimeout(1000);
    return b.build();
  }
}
