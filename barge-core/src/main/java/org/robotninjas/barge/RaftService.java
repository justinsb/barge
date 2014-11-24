package org.robotninjas.barge;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;

import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.proto.RaftEntry.Membership;
import org.robotninjas.barge.rpc.RaftClientProvider;
import org.robotninjas.barge.state.RaftStateContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * An instance of a set of replica managed through Raft protocol.
 * <p>
 * A {@link RaftService} instance is constructed by specific builders depending on: Communication protocol used,
 * persistent storage, network characteristics...
 * </p>
 */
public abstract class RaftService extends AbstractService {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftService.class);

  protected final RaftStateContext ctx;

  protected final Closer closer = Closer.create();
  
  protected RaftService(@Nonnull RaftStateContext ctx) {
    this.ctx = checkNotNull(ctx);
  }

  /**
   * Asynchronously executes an operation on the state machine managed by barge.
   * <p>
   * When the result becomes available, the operation is guaranteed to have been committed to the Raft cluster in such a
   * way that it is permanent and will be seen, eventually, by all present and future members of the cluster.
   * </p>
   *
   * @param operation
   *          an arbitrary operation to be sent to the <em>state machine</em> managed by Raft.
   * @return the result of executing the operation, wrapped in a {@link ListenableFuture}, that can be retrieved at a
   *         later point in time.
   * @throws org.robotninjas.barge.RaftException
   */
  public ListenableFuture<Object> commitAsync(byte[] operation) throws RaftException {
    return ctx.commitOperation(operation);
  }

  /**
   * Synchronously executes and operation on the state machine managed by barge.
   * <p>
   * This method is semantically equivalent to:
   * </p>
   * 
   * <pre>
   * commitAsync(op).get();
   * </pre>
   *
   * @param operation
   *          an arbitrary operation to be sent to the <em>state machine</em> managed by Raft.
   * @return the result of executing the operation, as returned by the state machine.
   * @throws RaftException
   * @throws InterruptedException
   *           if current thread is interrupted while waiting for the result to be available.
   */
  public Object commit(final byte[] operation) throws RaftException, InterruptedException {
    try {
      return commitAsync(operation).get();
    } catch (ExecutionException e) {
      propagateIfInstanceOf(e.getCause(), NotLeaderException.class);
      throw propagate(e.getCause());
    }
  }
  

  public RaftClusterHealth getClusterHealth() throws RaftException {
    return ctx.getClusterHealth();
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
    return "RaftService [ctx=" + ctx + "]";
  }

  public Replica self() {
    return ctx.self();
  }


  public void bootstrap(Membership membership) {
    ctx.bootstrap(membership);
  }


  public ListenableFuture<Boolean> setConfiguration(RaftMembership oldMembership, RaftMembership newMembership) {
    return ctx.setConfiguration(oldMembership, newMembership);
  }

  public Optional<Replica> getLeader() {
    return ctx.getLeader();
  }

  public static class Builder {
//    public ClusterConfig seedConfig;
    public RaftLog.Builder log;
    public RaftClientProvider raftClientProvider;
//    public ConfigurationState configurationState;
//    public BargeThreadPools threadPools;
//    public ListeningScheduledExecutorService raftExecutor;

    protected RaftStateContext buildRaftStateContext() {
      checkNotNull(log);
      checkNotNull(raftClientProvider);
//      checkNotNull(threadPools);

      Replica self = log.self();
      checkNotNull(self);
      
//      bind(Raft.class).to(RaftStateContext.class).asEagerSingleton();
//    // expose(Raft.class);
//
//    bind(ConfigurationState.class).toInstance(new ConfigurationState(self, seedConfig.allMembers, seedConfig.timeouts));
//    
     
//      RaftClientManager raftClientManager = new RaftClientManager(raftClientProvider, threadPools);
      return new RaftStateContext(log.build(), raftClientProvider, log.config);
    }
    
    protected Replica self() {
      return log.self();
    }
    
    protected String key() {
      return self().toString();
    }

    protected void populateDefaults() {
      
    
    }
  }

}
