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

package org.robotninjas.barge.netty.rpc;

import com.google.inject.PrivateModule;

import io.netty.channel.nio.NioEventLoopGroup;

import org.robotninjas.barge.BargeThreadPools;
import org.robotninjas.barge.rpc.RaftClientProvider;
import org.robotninjas.protobuf.netty.client.RpcClient;
import org.robotninjas.protobuf.netty.server.RpcServer;

import javax.annotation.Nonnull;

import java.net.SocketAddress;

import static com.google.common.base.Preconditions.checkNotNull;

public class RpcModule extends PrivateModule {

  private final SocketAddress saddr;
  private final BargeThreadPools bargeThreadPools;

  public RpcModule(@Nonnull SocketAddress saddr, BargeThreadPools bargeThreadPools) {
    this.saddr = checkNotNull(saddr);
    this.bargeThreadPools = checkNotNull(bargeThreadPools);
  }

  @Override
  protected void configure() {

    bind(BargeThreadPools.class)
          .toInstance(bargeThreadPools);
    expose(BargeThreadPools.class);
    
    RpcServer rpcServer = new RpcServer(bargeThreadPools.getEventLoopGroup(), saddr);
        
    bind(RpcServer.class)
        .toInstance(rpcServer);
    expose(RpcServer.class);

    bind(RaftClientProvider.class)
        .to(ProtoRpcRaftClientProvider.class)
        .asEagerSingleton();

    expose(RaftClientProvider.class);
    
    bind(RpcClient.class)
        .toInstance(new RpcClient(bargeThreadPools.getEventLoopGroup()));
    
  }

}
