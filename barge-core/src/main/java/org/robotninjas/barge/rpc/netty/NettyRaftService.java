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

import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.RaftService;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.StateMachine;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.proto.RaftProto;
import org.robotninjas.barge.rpc.RaftClient;
import org.robotninjas.barge.rpc.RaftClientProvider;
import org.robotninjas.barge.state.RaftStateContext;
import org.robotninjas.protobuf.netty.client.RpcClient;
import org.robotninjas.protobuf.netty.server.RpcServer;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
@Immutable
public class NettyRaftService extends RaftService {

  private final RpcServer rpcServer;

  NettyRaftService(@Nonnull RaftStateContext ctx, @Nonnull RpcServer rpcServer) {
    super(ctx);

    this.rpcServer = checkNotNull(rpcServer);

  }

  @Override
  protected void doStart() {

    try {

      ctx.init();

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

      notifyStopped();
    } catch (Exception e) {
      notifyFailed(e);
    }

  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder extends RaftService.Builder {
    public NioEventLoopGroup eventLoopGroup;
    public boolean closeEventLoopGroup;
    public RpcClient rpcClient;
    public String key;
    
    
    protected void populateDefaults() {
      super.populateDefaults();
      
      if (key == null) {
        key = self().toString();
      }
      
      if (eventLoopGroup == null) {
        eventLoopGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("pool-raft-" + key));
        closeEventLoopGroup = true;
      }
      
      if (this.rpcClient == null) {
        this.rpcClient = new RpcClient(eventLoopGroup);
      }
      
      if (this.raftClientProvider == null) {
        this.raftClientProvider = new ProtoRpcRaftClientProvider(rpcClient);
      }
    }
    
    public NettyRaftService build() {
      populateDefaults();
      
//      checkNotNull(logDir);
//      checkNotNull(seedConfig);
//      checkNotNull(seedConfig.self);

      Replica self = super.self();
      
      RpcServer rpcServer = new RpcServer(this.eventLoopGroup, self.address());


//    bind(RpcServer.class)
//        .toInstance(rpcServer);
//    expose(RpcServer.class);
//
//    bind(RaftClientProvider.class)
//        .to(ProtoRpcRaftClientProvider.class)
//        .asEagerSingleton();
//
//    expose(RaftClientProvider.class);
//    
//    bind(RpcClient.class)
//        .toInstance(new RpcClient(bargeThreadPools.getEventLoopGroup()));

      RaftStateContext context = super.buildRaftStateContext();
      //      Injector injector = Guice.createInjector(new NettyRaftModule(seedConfig, logDir, stateMachine));
      NettyRaftService nettyRaftService = new NettyRaftService(context, rpcServer);
      if (closeEventLoopGroup) {
        nettyRaftService.closer.register(makeCloseable(eventLoopGroup));
      }

      return nettyRaftService;
    }
  }

  public static Closeable makeCloseable(EventExecutorGroup eventLoopGroup) {
    return new Closeable() {
      @Override
      public void close() throws IOException {
        try {
          Future<?> future = eventLoopGroup.shutdownGracefully(1, 1, TimeUnit.SECONDS);
          future.await();
        } catch (Exception e) {
          throw new IOException("Error shutting down netty event loop",e);
        }
      }
    };
  }

}
