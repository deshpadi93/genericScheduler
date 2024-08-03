package testschedule;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

class SchedulerClass {

	private static final int CAPACITY = 10;

	private final BlockingQueue<TimedTask> queue = new PriorityBlockingQueue<>(CAPACITY, new Comparator<TimedTask>() {
		@Override
		public int compare(TimedTask s, TimedTask t) {
			return s.getScheduledTime().compareTo(t.getScheduledTime());
		}
	});

	private final Object lock = new Object();
	private volatile boolean running = true;

	public void start() throws InterruptedException {
        // each task runs on a separate thread without interrupting main thread
		// so that main thread is always ready to accept new tasks
		while (running) {
			TimedTask task = queue.take();
			if (task != null) {
				new Thread(new Runnable() {
					@Override
					public void run() {
						synchronized (lock) {
							while (!task.shouldRunNow()) {
								try {
									lock.wait(task.runFromNow());
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
							}
							lock.notify();
						}
						task.run();
					}
				}).start();
			}
			waitForNextTask();
		}
	}

	private void waitForNextTask() throws InterruptedException {
		synchronized (lock) {
			TimedTask nextTask = queue.peek();
			while (nextTask == null || !nextTask.shouldRunNow()) {
				if (nextTask == null) {
					lock.wait();
				} else {
					lock.wait(nextTask.runFromNow());
				}
				nextTask = queue.peek();
			}
		}
	}

	public void add(YourTask task) {
		add(task, 0);
	}

	public void add(YourTask task, long delayMs) {
		synchronized (lock) {
			queue.offer(TimedTask.fromTask(task, delayMs));
			lock.notify();
		}
	}

	public void addWithScheduleDate(YourTask task, Date date) {
		synchronized (lock) {
			queue.offer(TimedTask.fromTaskScheduleDateTime(task, date));
			lock.notify();
		}
	}

	public void stop() {
		this.running = false;
	}

	private static class TimedTask {
		// change task object according to your task object here
		private YourTask task;
		private Calendar scheduledTime;

		public TimedTask(YourTask task, Calendar scheduledTime) {
			this.task = task;
			this.scheduledTime = scheduledTime;
		}

		public static TimedTask fromTask(YourTask task, long delayMs) {
			Calendar now = Calendar.getInstance();
			now.setTimeInMillis(now.getTimeInMillis() + delayMs);
			return new TimedTask(task, now);
		}

		public static TimedTask fromTaskScheduleDateTime(YourTask task, Date date) {
			Calendar now = Calendar.getInstance();
			now.setTime(date);
			return new TimedTask(task, now);
		}

		public Calendar getScheduledTime() {
			return scheduledTime;
		}

		public long runFromNow() {
			return scheduledTime.getTimeInMillis() - Calendar.getInstance().getTimeInMillis();
		}

		public boolean shouldRunNow() {
			return runFromNow() <= 0;
		}

		public void run() {
			task.run();
		}
	}

	// scheduler function which accpts your task and schedule date time
	public void ScheduleTask(YourTask YourTask, String runOnce, String scheduleDateOrDelay)
			throws InterruptedException, ParseException {
		final SchedulerClass scheduler = new SchedulerClass();
		if (runOnce.equalsIgnoreCase("TRUE")) {
			SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss.SSS");
			Date date = formatter.parse(scheduleDateOrDelay);
			scheduler.addWithScheduleDate(YourTask, date);
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						System.out.println("Tasks " + YourTask.getName() + " getting added to thread no : "
								+ Thread.currentThread().getName());
						scheduler.start();
					} catch (InterruptedException e) {

					}
				}
			}).start();
		} else {
			scheduler.add(YourTask, Long.parseLong(scheduleDateOrDelay));
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						System.out.println("Tasks " + YourTask.getName() + " getting added with thread no : "
								+ Thread.currentThread().getName());
						scheduler.start();
					} catch (InterruptedException e) {

					}
				}
			}).start();
		}
	}

	public static void main(String[] args) throws InterruptedException, ParseException {
		SchedulerClass scheduler = new SchedulerClass();

		// sample task running with datetime
		// NOTE: Scheduled Time < Current Time then task would run immediately
		YourTask YourTask = new YourTask("Task wtih datetime");
		scheduler.ScheduleTask(YourTask, "TRUE", "03-08-2024 09:56:55.000");

		// sample task running with delay
		YourTask YourTask1 = new YourTask("Task with delay");
		scheduler.ScheduleTask(YourTask1, "FALSE", "1000");
	}
}

//Define your task which should be running at scheduled date time or after scheduled delay
class YourTask {
	private String name;

	public YourTask(String name) {
		this.name = name;
	}

	public String getName() {
		return this.name;
	}

	public void run() {
		System.out.println("Task " + name + " is running on thread " + Thread.currentThread().getName()
				+ " scheduled at time :" + Calendar.getInstance().getTimeInMillis());
	}
}
