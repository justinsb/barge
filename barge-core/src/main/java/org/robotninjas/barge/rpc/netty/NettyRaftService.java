/**
 * Copyright 2013 David Rusek <dave dot rusek at gmail dot com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.robotninjas.barge.rpc.netty;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.protobuf.Service;

import org.robotninjas.barge.BargeThreadPools;
import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.NotLeaderException;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftService;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.StateMachine;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.proto.RaftProto;
import org.robotninjas.barge.state.Raft.StateType;
import org.robotninjas.barge.state.RaftStateContext;
import org.robotninjas.barge.state.StateTransitionListener;
import org.robotninjas.protobuf.netty.server.RpcServer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.File;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.base.Throwables.propagateIfInstanceOf;

@ThreadSafe
@Immutable
public class NettyRaftService extends RaftService {

  private final RpcServer rpcServer;

  @Inject
  NettyRaftService(@Nonnull RpcServer rpcServer, @Nonnull BargeThreadPools bargeThreadPools,
      @Nonnull RaftStateContext ctx, @Nonnull RaftLog raftLog) {

    super(bargeThreadPools, ctx, raftLog);

    this.rpcServer = checkNotNull(rpcServer);

  }

  @Override
  protected void doStart() {

    try {

      ctx.init().get();

      configureRpcServer();
      rpcServer.startAsync().awaitRunning();

      notifyStarted();

    } catch (Exception e) {
      notifyFailed(e);
    }

  }

  private void configureRpcServer() {
    ProtobufRaftServiceEndpoint endpoint = new ProtobufRaftServiceEndpoint(ctx);
    Service replicaService = RaftProto.RaftService.newReflectiveService(endpoint);
    rpcServer.registerService(replicaService);
  }

  @Override
  protected void doStop() {

    try {
      rpcServer.stopAsync().awaitTerminated();
      ctx.stop();
      while (!ctx.isStopped()) {
        Thread.sleep(10);
      }
      raftLog.close();

      bargeThreadPools.close();
      ctx.stop();

      notifyStopped();
    } catch (Exception e) {
      notifyFailed(e);
    }

  }

  @Override
  public ListenableFuture<Object> commitAsync(final byte[] operation) throws RaftException {

    return ctx.commitOperation(operation);

  }

  @Override
  public Object commit(final byte[] operation) throws RaftException, InterruptedException {
    try {
      return commitAsync(operation).get();
    } catch (ExecutionException e) {
      propagateIfInstanceOf(e.getCause(), NotLeaderException.class);
      throw propagate(e.getCause());
    }
  }

  public boolean isLeader() {
    return StateType.LEADER == ctx.type();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private void addTransitionListener(StateTransitionListener listener) {
    ctx.addTransitionListener(listener);
  }

  public static class Builder {

    // private final NettyClusterConfig config;
    public ClusterConfig seedConfig;
    public File logDir;
    public StateTransitionListener listener;
    public StateMachine stateMachine;

    // protected Builder(NettyClusterConfig config) {
    // this.config = config;
    // }
    //
    // public Builder timeout(long timeout) {
    // this.timeout = timeout;
    // return this;
    // }
    //
    // public Builder logDir(File logDir) {
    // this.logDir = logDir;
    // return this;
    // }

    public NettyRaftService build() {
      // logDir = Files.createTempDir();
      checkNotNull(logDir);
      checkNotNull(seedConfig);
      checkNotNull(seedConfig.self);

      Injector injector = Guice.createInjector(new NettyRaftModule(seedConfig, logDir, stateMachine));
      NettyRaftService nettyRaftService = injector.getInstance(NettyRaftService.class);

      if (listener != null) {
        nettyRaftService.addTransitionListener(listener);
      }

      return nettyRaftService;
    }

    // public Builder transitionListener(StateTransitionListener listener) {
    // this.listener = listener;
    // return this;
    // }
  }

}
