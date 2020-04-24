# Using the Rate Limited Scheduled Executor

This project has two core components from an external view : the RateLimitedScheduledExecutors (executors) are responsible for taking scheduling requests, ordering them, manging how many should be executed and so on, and the 
 RateLimitedScheduledExecutorManager (manager), which is responsible for creating (executors), collecting records from them and executing them at the correct time.   

## Creating the Executor 

This module can be used with spring injection, guice injection, or no injection at all. The Guice and Spring libraries are marked as optional in our POM file, so you will need to include the library of your choice yourself.

##### No Injection library

if you want to just use our code directly, this is also very simple :

Create the executor manager with no arguments

```java
RateLimitedScheduledExecutorManager<ExtraRunnable> manager =  new RateLimitedScheduledExecutorManager<>();
```

         
If you want to configure the pools which the scheduling and execution is performed on, you need to pass a configured ExecutorSupplier to the manager (no bindings need to be in place for this to work)

```java   
ExecutorSupplier executorSupplier = new ExecutorSupplier();
executorSupplier.setExecutor(ForkJoinPool.commonPool());
executorSupplier.setScheduledExecutorService(Executors.newScheduledThreadPool(4));
RateLimitedScheduledExecutorManager<ExtraRunnable> manager =
   new RateLimitedScheduledExecutorManager<>(executorSupplier);
```

##### Spring injection
The simplest way of using the application with spring is to simply add the annotation 
```java
@ComponentScan("com.americanexpress.ratelimitedscheduler")
```

then inject the manager
```java
public TestApplication(RateLimitedScheduledExecutorManager<ExtraRunnable> manager) {  
    this.manager = manager;  
}
```

         
If you want to configure the pools which the scheduling and execution is performed on, we need the following beans injected

```java
@Bean  
@Qualifier("RateLimitedScheduledExecutor.taskScheduler")  
public ScheduledExecutorService getScheduledExecutorService() {  
  return new ScheduledThreadPoolExecutor(8);  
}  

@Bean  
@Qualifier("RateLimitedScheduledExecutor.taskExecutor")  
public Executor getForkJoinPoolExecutor() {  
  return ForkJoinPool.commonPool();  
}  
``` 

##### Guice injection
The simplest way of using the application with guice is to use your injector to create the manager - for example

```java
Injector injector = Guice.createInjector(new GuiceBindingModule());  
RateLimitedScheduledExecutorManager<ExtraRunnable> manager = injector.getInstance(RateLimitedScheduledExecutorManager.class);
```


         
If you want to configure the pools which the scheduling and execution is performed on, we need the following bindings in your binding module

```java 
bind(ScheduledExecutorService.class)  
   .annotatedWith(Names.named("RateLimitedScheduledExecutor.taskScheduler"))  
   .toInstance(new ScheduledThreadPoolExecutor(8));  
bind(Executor.class)  
   .annotatedWith(Names.named("RateLimitedScheduledExecutor.taskExecutor"))  
   .toInstance(ForkJoinPool.commonPool());  
``` 

## Using the executor

Once you have your manager, there are a few things you can do. Optionally, before you do anything, think about [tuning the scheduler](#tuning-the-buffer-and-interval-size).

##### Creating an executor
With that done, you can create your first RateLimitedScheduledExecutor (this is what you actually schedule tasks on). There is a one-to-many relationship of managers-to-executors to allow efficient execution of tasks. The parameter passed here is just used for logging
```java
RateLimitedScheduledExecutor executorA = manager.getRateLimitedScheduledExecutor("serviceA");
```
if you want to initialise the executor overriding the default values (described in the setters below), you can also call

```java
RateLimitedScheduledExecutor executorA = manager.getRateLimitedScheduledExecutor("serviceA", 10, Duration.ofMinutes(5), false);
```

where `10` is the maxTPS, `Duration.ofMinutes(5)` is the taskTimout, and `false` is the early dispatch setting

##### Disable possible early dispatch
before you schedule any tasks, you can also choose whether to allow tasks to run early. The scheduler runs efficiently by nanobatching an interval's worth of tasks together - by default this is one second. 
A task may be executed any time within that interval, so for example if you scheduled something for `12:00:00.995` it might execute anywhere between `12:00:00.000` and `12:00:00.999` (assuming no load issues - it could of course be later than that). 
The early running here may be an issue for some applications. if this is the case for you, call 
```java
executorA.setEnsureNoTaskDispatchedEarly(true);
```
before you start schedulling tasks, and your task as scheduled earlier would now execute anywhere between `12:00:01.000` and `12:00:01.999` (again, assuming no load issues, it could be later). It will not be called early.

##### Setting max TPS

Each executor can have its own rate limits, which can be set at any time with 
```java
executorA.setMaxTPS(100);
```
The change takes almost immediate effect (the next buffersize*millisPerInterval milliseconds of tasks will execute at the old rate). Setting this value to `Double.POSITIVE_INFINITY` (the default) disables throttling. Setting it to zero pauses the execution entirely.

It is important to note that if you use a custom interval, your effective TPS may be slightly lumpy. For example, if you have called `manager.setMillisPerInterval(999);` and then `executorA.setMaxTPS(3);` then then 999 intervals would have 3 transactions, and 1 would have 2 transactions. The overall TPS is correct, but you can see that it is not spread completely smoothly (due to how the tasks in a given interval are calculated)   

##### Setting the task timeout
The executor can be set up to time out a task which has not been attempted within a time window beyond the scheduled time. This also applies to things submitted for immediate execution. Calling
```java
executorA.setTaskTimeout(Duration.ofSeconds(5));
```
would stop tasks being attempted in an interval that started more than 5 seconds after they were scheduled - note they still might be attempted late if the executor behind our service is maxed out, or if the interval they fall in starts before the timeout time, but finishes after it

##### setting the sort method

If a TPS threshold is met, tasks can be sorted in any way. The sort method defaults to sorting by 
1. repeating tasks
2. tasks scheduled for immediate execution
3. other tasks

with the scheduled time (earliest first) being the order within each of those groups. We have some helper sorters in the TaskSorters class, which can be used like

```java
executorA.setSortMethod(TaskSorters.SORTED_BY_SCHEDULED_TIME_EARLIEST_FIRST);
```

or alternately you can implement your own, by casting the scheduled task to whatever you scheduled (this is fairly complex, and should have logic to cope with other tasks being scheduled. We would reccomend testing your sorting logic) - for example

```java
Comparator<ScheduledTask> sortMethod =
    (delayed1, delayed2) -> {
      //check we are comparing a runnable not a callable
      if (delayed1 instanceof ScheduledRunnableTask
          && delayed2 instanceof ScheduledRunnableTask) {
        Runnable runnable1 = ((ScheduledRunnableTask) delayed1).getRunnable();
        Runnable runnable2 = ((ScheduledRunnableTask) delayed2).getRunnable();
        //check its my type of runnable
        if (runnable1 instanceof RunnableTask && runnable2 instanceof RunnableTask) {
          //apply custom sort order
          return ((Task) runnable2).getId() - ((Task) runnable1).getId();
        }
      }
      //default if it is some other type of task
      return Long.compare(delayed2.getDelay(MILLISECONDS), delayed1.getDelay(MILLISECONDS));
    };
```

##### Scheduling

To schedule tasks for execution using any of the methods within the [ScheduledExecutorService](https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ScheduledExecutorService.html) interface, or its inherited [ExecutorService](https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ExecutorService.html) and [Executor](https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/Executor.html) interfaces. These will all react in the way that a normal executor service would act, except that the ``cancel(true);`` call on the `Future` returned will not interrupt a running task.    
```java
ScheduledRunnableTask scheduledRunnableTask = executorA.schedule(runnable, 100, MILLISECONDS);
ScheduledCallableTask scheduledCallableTask = executorA.schedule(callable, 100, MILLISECONDS);
RepeatingTask repeatingTask = scheduleAtFixedRate(runnable, 5, 5, SECONDS);
```

Each of these returns an extended version of the ``ScheduledFuture`` object, which has some features you can use in sorters if you wish (eg ``wasRequestedImmediately()`` and ``isRepeating()``)


##### Monitoring

We have two functions for monitoring an executor

```java
executorA.getNumberOfTasksAwaitingExecution();
```
will tell you the total number of tasks that are awaiting execution (not including those already sent to an intervalRunner) 

and

```java
executorA.getBacklogSize();
```

will tell you how many intervals full of backlog we have right now (ie, if we have TPS of 100, an overflow bucket of 210 items and 50 items per second scheduled, we would return 4).

Both of these are useful, and can be used to assess if you are having problems with your TPS limit beyond a small spike. 

##### Shutting things down

To close an executor, you can call
  
```java
executorA.shutdown();
```

this will block any future scheduling activities, stop any repeating tasks from continuing and then run every remaining task at the correct time. 

Calling 

```java
executorA.shutdownNow();
```

does the above, but additionally cancels any tasks which are yet to be submitted to an intervalRunner. 
 
 
```java
manager.shutdown();
```

will close the scheduledExecutorService behind all schedulers, if it was one created by our application (if you supplied it, this does nothing). After this, no threads should be running and your application can close gracefully 
 
##### Tuning the buffer and interval size 

The Rate Limited Scheduler has a default bufferSize of 2 and an interval size of 1 second; this means that 2 seconds before events are meant to start executing, they are collected together, TPS limit applied and so on. These items can be tuned by setting the system property 
```
RateLimitedScheduledExecutor.bufferSize
RateLimitedScheduledExecutor.millisPerInterval
```
Or, programatically, by calling 

```java
manager.setBufferSize(1);
manager.setMillisPerInterval(100);
```

before any executors have been created.

Setting the buffer higher means there is less likelyhood of scheduling being interrupted by things like long running GC. Setting it lower means the scheduler is more responsive - if you set it to 1, things scheduled within a second will happen in 1-2 intervals time, rather than 2-3 intervals time, and items which are cancelled last-second are removed from the TPS limit more efficiently.  We do not recommend a setting of 0 as it means that first part of each interval will be spent collecting tasks together.                                                                                                                                                                                                                                                                                                                                                                                                        

Setting the millis per second lower improves responsiveness at the cost of (slight) load, setting it higher reduces load but also reduces responsiveness.

If you don't have a problem with how the platform behaves, leaving these values as-is is probably a good idea. Extreme values may have unexpected results

##### Detection of other peers

The scheduler also supports the ability to detect other remote peers, and reduce the TPS accordingly. We have a single implementation of this, using Redis, which you can use as below :

```java 
JedisPool jedisPool = new JedisPool(new GenericObjectPoolConfig(), "redisHost", 12345, 30_000, "redisPassword");
Supplier<Jedis> jedisSupplier = jedisPool::getResource;
ConfiguredComponentSupplier configuredComponentSupplier = new ConfiguredComponentSupplier();
configuredComponentSupplier.setPeerFinder(new PeerFinderRedisImpl(jedisSupplier));
RateLimitedScheduledExecutorManager manager =  new RateLimitedScheduledExecutorManager(configuredComponentSupplier);
```

alternately you can roll your own service discovery tool by implementing the PeerFinder interface. By default, we tell the PeerFinder to update the network with its existance, and check for peers every 5 seconds, but this can be tuned by calling the `setPeerFinderIntervalMs(1000)` function

If you know how many peers you will have, we also support a way of having a fixed number of remote peers. An example of this is shown below 

```java 
ConfiguredComponentSupplier configuredComponentSupplier = new ConfiguredComponentSupplier();
configuredComponentSupplier.setPeerFinder(new PeerFinderStaticImpl(2));
RateLimitedScheduledExecutorManager manager =  new RateLimitedScheduledExecutorManager(configuredComponentSupplier);
```