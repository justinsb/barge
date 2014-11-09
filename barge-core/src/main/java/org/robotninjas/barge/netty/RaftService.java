package org.robotninjas.barge.netty;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.Callable;

import javax.annotation.Nonnull;
import javax.inject.Inject;

import org.robotninjas.barge.BargeThreadPools;
import org.robotninjas.barge.RaftClusterHealth;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftMembership;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.proto.RaftEntry.Membership;
import org.robotninjas.barge.state.RaftStateContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

/**
 * An instance of a set of replica managed through Raft protocol.
 * <p>
 *   A {@link RaftService} instance is constructed by specific builders depending on: Communication protocol used,
 *   persistent storage, network characteristics...
 * </p>
 */
public abstract class RaftService extends AbstractService {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftService.class);
  
  private final ListeningExecutorService executor;
//  private final RpcServer rpcServer;
  protected final RaftStateContext ctx;
  protected final RaftLog raftLog;

  protected final BargeThreadPools bargeThreadPools;
  
  @Inject
  RaftService(@Nonnull BargeThreadPools bargeThreadPools, @Nonnull RaftStateContext ctx, @Nonnull RaftLog raftLog) {

    this.bargeThreadPools = checkNotNull(bargeThreadPools);
    this.executor = checkNotNull(bargeThreadPools.getRaftExecutor());
//    this.rpcServer = checkNotNull(rpcServer);
    this.ctx = checkNotNull(ctx);
    this.raftLog = raftLog;

  }
  
  public RaftClusterHealth getClusterHealth() throws RaftException {

    // Make sure this happens on the Barge thread
    ListenableFuture<RaftClusterHealth> response = executor.submit(new Callable<RaftClusterHealth>() {
      @Override
      public RaftClusterHealth call() throws Exception {
        return ctx.getClusterHealth();
      }
    });

    return Futures.get(response, RaftException.class);
  }
    
  /**
   * Asynchronously executes an operation on the state machine managed by barge.
   * <p>
   * When the result becomes available, the operation is guaranteed to have been committed to the Raft
   * cluster in such a way that it is permanent and will be seen, eventually, by all present and
   * future members of the cluster.
   * </p>
   *
   * @param operation an arbitrary operation to be sent to the <em>state machine</em> managed by Raft.
   * @return the result of executing the operation, wrapped in a {@link ListenableFuture}, that can be retrieved
   *         at a later point in time.
   * @throws org.robotninjas.barge.RaftException
   */
  public abstract ListenableFuture<Object> commitAsync(byte[] operation) throws RaftException;

  /**
   * Synchronously executes and operation on the state machine managed by barge.
   * <p>
   * This method is semantically equivalent to:
   * </p>
   * <pre>
   *     commitAsync(op).get();
   * </pre>
   *
   * @param operation an arbitrary operation to be sent to the <em>state machine</em> managed by Raft.
   * @return the result of executing the operation, as returned by the state machine.
   * @throws RaftException
   * @throws InterruptedException if current thread is interrupted while waiting for the result to be available.
   */
  public abstract Object commit(byte[] operation) throws RaftException, InterruptedException;
  
  // TODO: This should probably take a RaftMembership
  public void bootstrap(Membership membership) {
    LOGGER.info("Bootstrapping log with {}", membership);
    if (!raftLog.isEmpty()) {
      LOGGER.warn("Cannot bootstrap, as raft log already contains data");
      throw new IllegalStateException();
    }
    raftLog.append(null, membership);
  }

  public ListenableFuture<Boolean> setConfiguration(final RaftMembership oldMembership,
      final RaftMembership newMembership) {

    // Make sure this happens on the Barge thread
    ListenableFuture<ListenableFuture<Boolean>> response = executor.submit(new Callable<ListenableFuture<Boolean>>() {
      @Override
      public ListenableFuture<Boolean> call() throws Exception {
        return ctx.setConfiguration(oldMembership, newMembership);
      }
    });

    return Futures.dereference(response);

  }

  public RaftMembership getClusterMembership() {
    return ctx.getConfigurationState().getClusterMembership();
  }

  public String getServerKey() {
    return ctx.getConfigurationState().self().getKey();
  }

  public boolean isLeader() {
    return ctx.isLeader();
  }

  @Override
  public String toString() {
    return "RaftService [log=" + raftLog + ", ctx=" + ctx + "]";
  }
  
  
}
