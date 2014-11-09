package org.robotninjas.barge.rpc.netty;

import com.google.inject.PrivateModule;

import java.io.File;

import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.RaftCoreModule;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.StateMachine;

public class NettyRaftModule extends PrivateModule {

  private final Replica self;
  private final File logDir;
  private final StateMachine stateMachine;
  private final ClusterConfig seedConfig;

  public NettyRaftModule(Replica self, ClusterConfig seedConfig, File logDir, StateMachine stateMachine) {
    this.self = self;
    this.seedConfig = seedConfig;
    this.logDir = logDir;
    this.stateMachine = stateMachine;
  }

  @Override
  protected void configure() {

    RaftCoreModule.Builder options = RaftCoreModule.builder();
    options.self = self;
    options.seedConfig = seedConfig;
    options.logDir = logDir;
    options.stateMachine = stateMachine;
//                                .withTimeout(timeout)
//                                .withSelf(self
//                                .withLogDir(logDir)
//                                .withStateMachine(stateMachine);
                                
    
    install(options.build());

    install(new RaftProtoRpcModule(options));

    bind(NettyRaftService.class);
    expose(NettyRaftService.class);
  }

}
