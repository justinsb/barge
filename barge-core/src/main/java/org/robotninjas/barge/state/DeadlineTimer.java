package org.robotninjas.barge.state;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import com.google.common.base.Optional;

import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@NotThreadSafe
class DeadlineTimer {

  private final ScheduledExecutorService scheduler;
  private final Runnable action;
  private final long timeout;
  private boolean started = false;
  private Optional<ScheduledFuture> future;

  DeadlineTimer(ScheduledExecutorService scheduler, Runnable action, long timeout) {
    this.scheduler = scheduler;
    this.action = action;
    this.timeout = timeout;
    this.future = Optional.absent();
  }

  public void start() {
    checkState(!started);
    started = true;
    reset();
  }

  public void reset() {
    checkState(started);
    if (future.isPresent()) {
      future.get().cancel(false);
    }
    ScheduledFuture<?> scheduled = scheduler.schedule(action, timeout, MILLISECONDS);
    future = Optional.of((ScheduledFuture) scheduled);
  }

  public void cancel() {
    checkState(started);
    if (future.isPresent()) {
      future.get().cancel(false);
    }
  }

  public static DeadlineTimer start(ScheduledExecutorService scheduler, Runnable action, long timeout) {
    DeadlineTimer t = new DeadlineTimer(scheduler, action, timeout);
    t.start();
    return t;
  }

}
