package com.tsa.mqmonitor.sampling;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;

/** Single-threaded TaskScheduler double: nothing runs until the test advances time or fires. */
final class RecordingTaskScheduler implements TaskScheduler {
  private final MutableClock clock;
  private final List<Task> tasks = new ArrayList<>();

  RecordingTaskScheduler(MutableClock clock) {
    this.clock = clock;
  }

  List<Task> periodicTasks() {
    return tasks.stream().filter(t -> t.period != null).toList();
  }

  Duration period(int index) {
    return periodicTasks().get(index).period;
  }

  void advance(Duration duration) {
    Instant target = clock.instant().plus(duration);
    Optional<Task> due;
    while ((due = nextDue(target)).isPresent()) {
      Task task = due.get();
      clock.set(task.nextRun);
      if (task.period == null) {
        tasks.remove(task);
      } else {
        task.nextRun = task.nextRun.plus(task.period);
      }
      task.runnable.run();
    }
    clock.set(target);
  }

  void fireAll() {
    for (Task task : List.copyOf(periodicTasks())) {
      if (tasks.contains(task)) {
        task.runnable.run();
      }
    }
  }

  private Optional<Task> nextDue(Instant target) {
    return tasks.stream()
        .filter(t -> !t.nextRun.isAfter(target))
        .min(Comparator.comparing(t -> t.nextRun));
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, Duration delay) {
    return add(new Task(task, clock.instant(), delay));
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, Instant startTime, Duration delay) {
    return add(new Task(task, startTime, delay));
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable task, Instant startTime) {
    return add(new Task(task, startTime, null));
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable task, Trigger trigger) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Instant startTime, Duration period) {
    throw new UnsupportedOperationException("the registry must not use fixed rate");
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Duration period) {
    throw new UnsupportedOperationException("the registry must not use fixed rate");
  }

  private ScheduledFuture<?> add(Task task) {
    tasks.add(task);
    return task;
  }

  final class Task implements ScheduledFuture<Object> {
    final Runnable runnable;
    final Duration period;
    Instant nextRun;
    boolean cancelled;

    Task(Runnable runnable, Instant firstRun, Duration period) {
      this.runnable = runnable;
      this.nextRun = firstRun;
      this.period = period;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      cancelled = true;
      return tasks.remove(this);
    }

    @Override
    public boolean isCancelled() {
      return cancelled;
    }

    @Override
    public boolean isDone() {
      return cancelled || !tasks.contains(this);
    }

    @Override
    public Object get() {
      return null;
    }

    @Override
    public Object get(long timeout, TimeUnit unit) {
      return null;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(Duration.between(clock.instant(), nextRun));
    }

    @Override
    public int compareTo(Delayed o) {
      return Long.compare(getDelay(TimeUnit.NANOSECONDS), o.getDelay(TimeUnit.NANOSECONDS));
    }
  }
}
