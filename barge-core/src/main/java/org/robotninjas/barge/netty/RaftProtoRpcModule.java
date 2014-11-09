package org.robotninjas.barge.netty;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.AbstractModule;

import io.netty.channel.nio.NioEventLoopGroup;

import org.robotninjas.barge.BargeThreadPools;
import org.robotninjas.barge.RaftCoreModule;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.netty.rpc.RpcModule;

class RaftProtoRpcModule extends AbstractModule {

  private final Replica self;

  private Optional<NioEventLoopGroup> eventLoopGroup = Optional.absent();

  private Optional<ListeningExecutorService> stateMachineExecutor;

  public RaftProtoRpcModule(RaftCoreModule.Builder builder) {
    this.self = builder.self;
    this.stateMachineExecutor = builder.stateMachineExecutor;
  }

  @Override
  protected void configure() {
    final NioEventLoopGroup eventLoop = initializeEventLoop();

    BargeThreadPools bargeThreadPools = new BargeThreadPools(self.toString(), Optional.of(eventLoop), stateMachineExecutor);
    install(new RpcModule(self.address(), bargeThreadPools));
    //expose(BargeThreadPools.class);

//    expose(RpcServer.class);
//    expose(RaftClientProvider.class);
  }

  private NioEventLoopGroup initializeEventLoop() {
    final NioEventLoopGroup eventLoop;

    if (eventLoopGroup.isPresent()) {
      eventLoop = eventLoopGroup.get();
    } else {
      // TODO: Verify that we need to restrict to a single thread
      // (as otherwise thread guarantees aren't enforced?)
      eventLoop = new NioEventLoopGroup(1);
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          eventLoop.shutdownGracefully();
        }
      });
    }
    return eventLoop;
  }

}
