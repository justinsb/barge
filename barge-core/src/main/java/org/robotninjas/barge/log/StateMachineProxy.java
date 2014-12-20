package org.robotninjas.barge.log;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;

import org.robotninjas.barge.StateMachine;
import org.robotninjas.barge.StateMachine.Snapshotter;
import org.robotninjas.barge.proto.RaftEntry.SnapshotInfo;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
public class StateMachineProxy  {

  private final ListeningExecutorService stateMachineExecutor;
  private final StateMachine stateMachine;
  private final boolean closeStateMachineExecutor;

  public StateMachineProxy(@Nonnull StateMachine stateMachine, @Nonnull ListeningExecutorService stateMachineExecutor, boolean closeStateMachineExecutor) {
    this.closeStateMachineExecutor = closeStateMachineExecutor;
    this.stateMachineExecutor = checkNotNull(stateMachineExecutor);
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

//  @Nonnull
//  public ListenableFuture installSnapshot() {
//    return Futures.immediateFailedFuture(new IllegalStateException());
//  }

  public void close() throws InterruptedException {
    if (closeStateMachineExecutor) {
      this.stateMachineExecutor.shutdown();
      this.stateMachineExecutor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  public ListenableFuture<Object> gotSnapshot(SnapshotInfo snapshotInfo) {
    checkNotNull(snapshotInfo);
    return submit(new Callable<Object>() {
      @Override
      public Object call() {
        stateMachine.gotSnapshot(snapshotInfo);
        return Boolean.TRUE;
      }
    });
  }

}
