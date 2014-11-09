///**
// * Copyright 2013-2014 David Rusek <dave dot rusek at gmail dot com>
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.robotninjas.barge.state;
//
//import org.robotninjas.barge.BargeThreadPools;
//import org.robotninjas.barge.log.RaftLog;
//import org.robotninjas.barge.rpc.Client;
//
//import javax.annotation.Nonnegative;
//import javax.inject.Inject;
//
//class DefaultStateFactory implements StateFactory {
//
//
//  private final RaftLog log;
//  private final BargeThreadPools threadPools;
//  private final long timeout;
//  private final Client client;
//
//  @Inject
//  public DefaultStateFactory(RaftLog log, BargeThreadPools threadPools,
//                             @ElectionTimeout @Nonnegative long timeout,
//                             Client client) {
//    this.log = log;
//    this.threadPools = threadPools;
//    this.timeout = timeout;
//    this.client = client;
//  }
//
//  @Override
//  public State makeState(RaftStateContext.StateType state) {
//    switch (state) {
//      case START:
//        return new Start(log);
//      case FOLLOWER:
//        return new Follower(log, threadPools, timeout);
//      case LEADER:
//        return new Leader(log, threadPools, timeout);
//      case CANDIDATE:
//        return new Candidate(log, threadPools, timeout, client);
//      case STOPPED:
//        return new Stopped(log);
//      default:
//        throw new IllegalStateException("the impossible happpened, unknown state type " + state);
//    }
//  }
//}
