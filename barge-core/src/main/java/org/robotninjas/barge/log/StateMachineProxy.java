package org.robotninjas.barge.log;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.inject.Inject;

import org.robotninjas.barge.BargeThreadPools;
import org.robotninjas.barge.StateMachine;
import org.robotninjas.barge.StateMachine.Snapshotter;
import org.robotninjas.barge.proto.RaftEntry.SnapshotInfo;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
public class StateMachineProxy {

  private final Executor stateMachineExecutor;
  private final StateMachine stateMachine;

  @Inject
  StateMachineProxy(@Nonnull BargeThreadPools bargeThreadPools, @Nonnull StateMachine stateMachine) {
    this.stateMachineExecutor = checkNotNull(bargeThreadPools.getStateMachineExecutor());
    this.stateMachine = checkNotNull(stateMachine);
  }

  private <V> ListenableFuture<V> submit(Callable<V> runnable) {

    ListenableFutureTask<V> operation =
        ListenableFutureTask.create(runnable);

    stateMachineExecutor.execute(operation);

    return operation;

  }

  @Nonnull
  public ListenableFuture<Object> dispatchOperation(@Nonnull final ByteBuffer op) {
    checkNotNull(op);
    return submit(new Callable<Object>() {
      @Override
      public Object call() {
        return stateMachine.applyOperation(op.asReadOnlyBuffer());
      }
    });
  }

  @Nonnull
  public ListenableFuture<Snapshotter> prepareSnapshot(final long currentTerm, final long currentIndex) throws IOException {
    return submit(new Callable<Snapshotter>() {
      @Override
      public Snapshotter call() {
        Snapshotter snapshotter = stateMachine.prepareSnapshot(currentTerm, currentIndex);
        return snapshotter;
      }
    });
  }

  @Nonnull
  public ListenableFuture installSnapshot() {
    return Futures.immediateFailedFuture(new IllegalStateException());
  }

}
