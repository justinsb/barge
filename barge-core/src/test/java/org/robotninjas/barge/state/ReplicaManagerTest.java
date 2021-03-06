package org.robotninjas.barge.state;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.GetEntriesResult;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.rpc.RaftClient;
import org.robotninjas.barge.rpc.RaftClientProvider;

import java.util.Collections;

import static junit.framework.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.robotninjas.barge.proto.RaftEntry.Entry;
import static org.robotninjas.barge.proto.RaftProto.AppendEntries;
import static org.robotninjas.barge.proto.RaftProto.AppendEntriesResponse;

public class ReplicaManagerTest {

//  private static final ClusterConfig config = ClusterConfigStub.getStub();
  private static final Replica SELF = Replica.fromString("local"); //config.local();
  private static final Replica FOLLOWER = Replica.fromString("remote"); //config.getReplica("remote");

  private
  @Mock
  RaftClientProvider mockRaftClientProvider;
  private
  @Mock
  RaftLog mockRaftLog;

  @Before
  public void initMocks() {

    MockitoAnnotations.initMocks(this);

    when(mockRaftLog.self()).thenReturn(SELF);
    when(mockRaftLog.getLastLogTerm()).thenReturn(0L);
    when(mockRaftLog.getLastLogIndex()).thenReturn(0L);
    when(mockRaftLog.getCurrentTerm()).thenReturn(1L);

  }

  @Test
  public void testInitialSendOutstanding() {

    ListenableFuture<AppendEntriesResponse> mockResponse = mock(ListenableFuture.class);
    RaftClient mockFollowerRaftClient = mock(RaftClient.class);
//    when(mockRaftClientProvider.get(eq(FOLLOWER))).thenReturn(mockFollowerRaftClient);
    when(mockFollowerRaftClient.appendEntries(any(AppendEntries.class))).thenReturn(mockResponse);

    GetEntriesResult entriesResult = new GetEntriesResult(0l, 0l, Collections.<Entry>emptyList());
    when(mockRaftLog.getEntriesFrom(anyLong(), anyInt())).thenReturn(entriesResult);

    ReplicaManager replicaManager = new ReplicaManager(mockRaftLog, mockFollowerRaftClient, FOLLOWER);
    ListenableFuture f1 = replicaManager.requestUpdate();

    AppendEntries appendEntries =
      AppendEntries.newBuilder()
        .setLeaderId(SELF.toString())
        .setCommitIndex(0)
        .setPrevLogIndex(0)
        .setPrevLogTerm(0)
        .setTerm(1)
        .build();

//    verify(mockRaftClientProvider, times(1)).get(FOLLOWER);
    verify(mockFollowerRaftClient, times(1)).appendEntries(appendEntries);
    verifyNoMoreInteractions(mockFollowerRaftClient);

    verify(mockRaftLog, times(1)).getEntriesFrom(1, 1);

    assertTrue(replicaManager.isRunning());
    assertFalse(replicaManager.isRequested());
    assertEquals(1, replicaManager.getNextIndex());

    ListenableFuture f2 = replicaManager.requestUpdate();

    assertNotSame(f1, f2);
    assertTrue(replicaManager.isRunning());
    assertTrue(replicaManager.isRequested());
    assertEquals(1, replicaManager.getNextIndex());

  }

  @Test
  public void testFailedAppend() {

    SettableFuture<AppendEntriesResponse> response = SettableFuture.create();
    RaftClient mockFollowerRaftClient = mock(RaftClient.class);
    when(mockRaftClientProvider.get(eq(FOLLOWER))).thenReturn(mockFollowerRaftClient);
    when(mockFollowerRaftClient.appendEntries(any(AppendEntries.class)))
      .thenReturn(response)
      .thenReturn(mock(ListenableFuture.class));


    GetEntriesResult entriesResult = new GetEntriesResult(0l, 0l, Collections.<Entry>emptyList());
    when(mockRaftLog.getEntriesFrom(anyLong(), anyInt())).thenReturn(entriesResult);

    ReplicaManager replicaManager = new ReplicaManager(mockRaftLog, mockFollowerRaftClient, FOLLOWER);

    replicaManager.requestUpdate();

    AppendEntries appendEntries =
      AppendEntries.newBuilder()
        .setLeaderId(SELF.toString())
        .setCommitIndex(0)
        .setPrevLogIndex(0)
        .setPrevLogTerm(0)
        .setTerm(1)
        .build();

    assertTrue(replicaManager.isRunning());
    assertFalse(replicaManager.isRequested());
    assertEquals(1, replicaManager.getNextIndex());

    AppendEntriesResponse appendEntriesResponse =
      AppendEntriesResponse.newBuilder()
        .setLastLogIndex(0L)
        .setTerm(1)
        .setSuccess(false)
        .build();

    response.set(appendEntriesResponse);

    verify(mockFollowerRaftClient, times(2)).appendEntries(appendEntries);
    verifyNoMoreInteractions(mockFollowerRaftClient);

    verify(mockRaftLog, times(2)).getEntriesFrom(1, 1);

    assertTrue(replicaManager.isRunning());
    assertFalse(replicaManager.isRequested());
    assertEquals(1, replicaManager.getNextIndex());

  }

  @Test
  public void testSuccessfulAppend() {

    SettableFuture<AppendEntriesResponse> response = SettableFuture.create();
    RaftClient mockFollowerRaftClient = mock(RaftClient.class);
    when(mockRaftClientProvider.get(eq(FOLLOWER))).thenReturn(mockFollowerRaftClient);
    when(mockFollowerRaftClient.appendEntries(any(AppendEntries.class)))
      .thenReturn(response)
      .thenReturn(mock(ListenableFuture.class));

    Entry entry = Entry.newBuilder()
      .setTerm(1)
      .setCommand(ByteString.EMPTY)
      .build();

    GetEntriesResult entriesResult = new GetEntriesResult(0l, 0l, Lists.newArrayList(entry));
    when(mockRaftLog.getEntriesFrom(anyLong(), anyInt())).thenReturn(entriesResult);

    ReplicaManager replicaManager = new ReplicaManager(mockRaftLog, mockFollowerRaftClient, FOLLOWER);

    replicaManager.requestUpdate();

    AppendEntries appendEntries =
      AppendEntries.newBuilder()
        .setLeaderId(SELF.toString())
        .setCommitIndex(0)
        .setPrevLogIndex(0)
        .setPrevLogTerm(0)
        .setTerm(1)
        .addEntries(entry)
        .build();

    assertTrue(replicaManager.isRunning());
    assertFalse(replicaManager.isRequested());
    assertEquals(1, replicaManager.getNextIndex());

    AppendEntriesResponse appendEntriesResponse =
      AppendEntriesResponse.newBuilder()
        .setLastLogIndex(0L)
        .setTerm(1)
        .setSuccess(true)
        .build();

    response.set(appendEntriesResponse);

    verify(mockFollowerRaftClient, times(1)).appendEntries(appendEntries);
    verifyNoMoreInteractions(mockFollowerRaftClient);

    verify(mockRaftLog, times(1)).getEntriesFrom(1, 1);

    assertFalse(replicaManager.isRunning());
    assertFalse(replicaManager.isRequested());
    assertEquals(1, replicaManager.getNextIndex());

  }


}
