package org.robotninjas.barge.netty;

import javax.annotation.Nonnull;

import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftMembership;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.StateMachine;
import org.robotninjas.barge.proto.RaftEntry.Membership;
import org.robotninjas.barge.rpc.netty.NettyRaftService;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

public class SimpleCounterMachine implements StateMachine {

  private final int id;
  private final GroupOfCounters groupOfCounters;

  private long counter;
  private File logDirectory;
  private NettyRaftService service;
  Replica self;
  private ClusterConfig seedConfig;

  
  @Override
  public String toString() {
    return "id=" + id + "; state=" + service;
  }

  public SimpleCounterMachine(int id, ClusterConfig config, GroupOfCounters groupOfCounters) {
//    checkArgument(id >= 0 && id < replicas.size(), "replica id " + id + " should be between 0 and " + replicas.size());

    this.groupOfCounters = groupOfCounters;
    this.id = id;
    this.self = config.self;
    this.seedConfig = config;
  }

  @SuppressWarnings({"ConstantConditions", "ResultOfMethodCallIgnored"})
  public static void delete(File directory) {
    for (File file : directory.listFiles()) {
      if (file.isFile()) {
        file.delete();
      } else {
        delete(file);
      }
    }
    directory.delete();
  }

  @Override
  public Object applyOperation(@Nonnull ByteBuffer entry) {
    this.counter += entry.get();
    return this.counter;
  }

  public void startRaft() {
//    int clusterSize = replicas.size();
//    Replica[] configuration = new Replica[clusterSize - 1];
//    for (int i = 0; i < clusterSize - 1; i++) {
//      configuration[i] = replicas.get((id + i + 1) % clusterSize);
//    }
//
//    NettyClusterConfig config1 = NettyClusterConfig.from(replicas.get(id % clusterSize), configuration);

    NettyRaftService.Builder b = NettyRaftService.newBuilder();
    b.self = self;
    b.seedConfig = seedConfig;
    b.logDir = logDirectory;
    b.listener = groupOfCounters;
    b.stateMachine = this;
    NettyRaftService service1 = b.build();

    service1.startAsync().awaitRunning();
    this.service = service1;
  }

  public File makeLogDirectory(File parentDirectory) throws IOException {
    this.logDirectory = new File(parentDirectory, "log" + id);

    if (logDirectory.exists()) {
      delete(logDirectory);
    }

    if (!logDirectory.exists() && !logDirectory.mkdirs()) {
      throw new IllegalStateException("cannot create log directory " + logDirectory);
    }

    return logDirectory;
  }


  public void commit(byte[] bytes) throws InterruptedException, RaftException {
    service.commit(bytes);
  }

  public void stop() {
    service.stopAsync().awaitTerminated();
  }

  public void deleteLogDirectory() {
    delete(logDirectory);
  }

  /**
   * Wait at most {@code timeout} for this counter to reach value {@code increments}.
   *
   * @param increments value expected for counter.
   * @param timeout    timeout in ms.
   * @throws IllegalStateException if {@code expected} is not reached at end of timeout.
   */
  public void waitForValue(final long target, long timeout) {
    new Prober(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return target == counter;
      }
    }).probe(timeout);
  }

  public boolean isLeader() {
    return service.isLeader();
  }

//  public void bootstrap(Membership membership) {
//    this.service.bootstrap(membership);
//  }

  public ListenableFuture<Boolean>  setConfiguration(RaftMembership oldMembership, RaftMembership newMembership) {
    return this.service.setConfiguration(oldMembership, newMembership);
  }

  public RaftMembership getClusterMembership() {
   return this.service.getClusterMembership();
  }

}
