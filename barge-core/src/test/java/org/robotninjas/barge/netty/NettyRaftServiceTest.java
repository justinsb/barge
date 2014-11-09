package org.robotninjas.barge.netty;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.robotninjas.barge.Replica;

import com.google.common.collect.Lists;

import java.io.File;
import java.util.Collections;
import java.util.List;

public class NettyRaftServiceTest {

  private static final File target = new File(System.getProperty("basedir", "."), "target");

  @Rule
  public GroupOfCounters counters = new GroupOfCounters(target);

  @Test(timeout = 30000)
  public void canRun3RaftInstancesReachingCommonState() throws Exception {
    counters.addServer(0, 0);
    counters.bootstrap(0);

    counters.waitForLeaderElection();
    counters.printState();
    assertEquals(1, counters.clusterMemberCount());
    
    counters.addServer(1, 0, 1, 2);
    counters.addServer(2, 0, 1, 2);
  
    counters.waitForLeaderElection();
    counters.printState();
    //assertEquals(1, counters.clusterMemberCount());
    
    {
      List<Replica> newMembers = Lists.newArrayList();
      for (SimpleCounterMachine counter : counters.servers.values()) {
        newMembers.add(counter.self);
      }
      counters.changeCluster(newMembers);
    }
    
    counters.waitForLeaderElection();
    counters.printState();
    assertEquals(3, counters.clusterMemberCount());
    
    
    int increments = 10;

    for (int i = 0; i < increments; i++) {
      counters.commitToLeader(new byte[]{1});
    }

    counters.waitAllToReachValue(increments, 10000);
  }


}
