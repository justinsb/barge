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
//import io.netty.channel.nio.NioEventLoopGroup;
//
//import org.junit.Before;
//import org.junit.Ignore;
//import org.junit.Test;
//import org.robotninjas.barge.BargeThreadPools;
//import org.robotninjas.barge.proto.RaftProto;
//
//import com.google.common.base.Optional;
//import com.google.common.util.concurrent.ListeningExecutorService;
//
//import static org.mockito.Mockito.*;
//import static org.robotninjas.barge.state.Raft.StateType.*;
//
////@Ignore
//public class RaftStateContextTest {
//
//  private final StateFactory factory = mock(StateFactory.class);
//
//  private final State start = mock(State.class);
//  private final State follower = mock(State.class);
//  private final State candidate = mock(State.class);
//
//  private final BargeThreadPools bargeThreadPools = new BargeThreadPools(RaftStateContext.class.getSimpleName(), Optional.<NioEventLoopGroup>absent(), Optional.<ListeningExecutorService>absent()); //mock(BargeThreadPools.class);
//  private final ConfigurationState configurationState = mock(ConfigurationState.class);
//  
//  private final StateTransitionListener transitionListener = mock(StateTransitionListener.class);
//
//  private final RaftStateContext context = new RaftStateContext("mockstatecontext", configurationState, bargeThreadPools);
//
//  @Before
//  public void setup() {
//    when(factory.makeState(START)).thenReturn(start);
//    when(factory.makeState(FOLLOWER)).thenReturn(follower);
////    executor.start();
//  }
//
//  @Test
//  public void notifiesSuccessfulStateTransitionFromNullToStartToRegisteredListener() throws Exception {
//    context.addTransitionListener(transitionListener);
//
//    context.setState(null, START);
//
//    verify(transitionListener).changeState(context, null, START);
//  }
//
//  @Test
//  public void throwsExceptionAndNotifiesInvalidTransitionToRegisteredListenerGivenUnexpectedPreviousState() throws Exception {
//    when(candidate.type()).thenReturn(CANDIDATE);
//    context.addTransitionListener(transitionListener);
//
//    context.setState(null, START);
//
//    try {
//      context.setState(candidate, LEADER);
//    } catch (IllegalStateException e) {
//      verify(transitionListener).invalidTransition(context, START, CANDIDATE);
//    }
//  }
//
//  @Test
//  public void triggersInitOnNewStateWhenTransitioning() throws Exception {
//    context.setState(null, START);
//
//    verify(start).init(context);
//  }
//
//  @Test
//  public void delegatesAppendEntriesRequestToCurrentState() throws Exception {
//    RaftProto.AppendEntries appendEntries = RaftProto.AppendEntries.getDefaultInstance();
//    context.setState(null, FOLLOWER);
//
//    context.appendEntries(appendEntries);
//
//    verify(follower).appendEntries(context, appendEntries);
//  }
//
//  @Test
//  public void delegateRequestVoteToCurrentState() throws Exception {
//    RaftProto.RequestVote requestVote = RaftProto.RequestVote.getDefaultInstance();
//    context.setState(null, FOLLOWER);
//
//    context.requestVote(requestVote);
//
//    verify(follower).requestVote(context, requestVote);
//  }
//
//  @Test
//  @Ignore
//  public void delegateCommitOperationToCurrentState() throws Exception {
//    byte[] bytes = new byte[]{1};
//    context.setState(null, FOLLOWER);
//
//    context.commitOperation(bytes);
//
//    verify(follower).commitOperation(context, bytes);
//  }
//}
