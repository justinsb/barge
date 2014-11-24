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

package org.robotninjas.barge.log;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.robotninjas.barge.Replica;
import org.robotninjas.barge.StateMachine;
import org.robotninjas.barge.proto.RaftEntry.Membership;
import org.robotninjas.barge.proto.RaftProto.AppendEntries;
import org.robotninjas.barge.state.ConfigurationState;

import io.netty.util.concurrent.DefaultThreadFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.concurrent.Executors;

@NotThreadSafe
public interface RaftLog {

  long currentTerm();

  void currentTerm(long currentTerm);

  GetEntriesResult getEntriesFrom(@Nonnegative long beginningIndex, @Nonnegative int max);

  public String getName();

  public long lastLogTerm();

  public long lastLogIndex();

  public long commitIndex();

  public void setCommitIndex(long index);

  // TODO: Remove?
  public Replica self();

  // TODO: This seems wrong / should have a term?
  @Nonnull
  public Optional<Replica> votedFor();

  // TODO: This seems wrong / should have a term?
  // TODO: Create one append method?
  public void votedFor(@Nonnull Optional<Replica> vote);

  // TODO: Create one append method?
  public ListenableFuture<Object> append(@Nonnull byte[] operation, @Nonnull Membership membership);

  // TODO: Create one append method?
  public boolean append(@Nonnull AppendEntries appendEntries);

  public void close() throws Exception;

  public void load() throws IOException;

  boolean isEmpty();

  public abstract static class Builder {
    public StateMachine stateMachine;
    public ConfigurationState config;

    public ListeningExecutorService stateMachineExecutor;

    protected StateMachineProxy stateMachineProxy;

    @Nonnull
    public abstract RaftLog build();

    protected void buildDefaults() {
      String key = self().getKey();

      boolean closeStateMachineExecutor = false;
      if (stateMachineExecutor == null) {
        stateMachineExecutor = MoreExecutors.listeningDecorator(Executors
            .newSingleThreadExecutor(new DefaultThreadFactory("pool-state-worker-" + key)));
        closeStateMachineExecutor = true;
      }

      stateMachineProxy = new StateMachineProxy(stateMachine, stateMachineExecutor, closeStateMachineExecutor);
    }

    public Replica self() {
      return config.self();
    }

  }
}
