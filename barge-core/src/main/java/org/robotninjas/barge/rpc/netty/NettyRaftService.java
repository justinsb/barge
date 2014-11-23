/**
 * Copyright 2014 Justin Santa Barbara
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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.protobuf.Service;

import org.robotninjas.barge.BargeThreadPools;
import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.RaftService;
import org.robotninjas.barge.StateMachine;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.proto.RaftProto;
import org.robotninjas.barge.state.RaftStateContext;
import org.robotninjas.protobuf.netty.server.RpcServer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.File;

import static com.google.common.base.Preconditions.checkNotNull;

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

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    public ClusterConfig seedConfig;
    public File logDir;
    public StateMachine stateMachine;


    public NettyRaftService build() {
      checkNotNull(logDir);
      checkNotNull(seedConfig);
      checkNotNull(seedConfig.self);

      Injector injector = Guice.createInjector(new NettyRaftModule(seedConfig, logDir, stateMachine));
      NettyRaftService nettyRaftService = injector.getInstance(NettyRaftService.class);

      return nettyRaftService;
    }
  }

}
