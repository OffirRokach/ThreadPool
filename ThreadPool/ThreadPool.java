/******************************************************************************
 * File name: ThreadPool
 * Owner: Offir Rokach
 * Reviewer: Tal
 * Review status: Approved
 * Date: 13/03/2025
 * Last Update: 18/03/2025
 ******************************************************************************/

package il.co.ilrd.ThreadPool;

import il.co.ilrd.WaitablePQueue.WaitablePQueueCV;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import static java.util.concurrent.TimeUnit.*;

public class ThreadPool implements Executor {

    private final WaitablePQueueCV<Task<?>> taskWaitablePQ = new WaitablePQueueCV<>(new TPComparator());
    private AtomicInteger numOfThreads = new AtomicInteger(0);
    private boolean isShutdown = false;
    private boolean isPaused = false;
    private final Semaphore pauseSemaphore = new Semaphore(0);
    private final Semaphore terminateSemaphore = new Semaphore(0);
    private final static int HIGHEST_PRIORITY = Priority.HIGH.ordinal() + 10;
    private final static int LOWEST_PRIORITY = Priority.LOW.ordinal() - 10;
    private final Callable<Object> killingApple = () -> {
        ((WorkingThread)Thread.currentThread()).isRunning = false;
        return null;
    };
    private final Callable<Object> pausingApple = () -> {
        pauseSemaphore.acquire();
        return null;
    };

    public enum Priority {
        LOW,
        MEDIUM,
        HIGH
    }

    public ThreadPool(){
        this(calcAvailableProcessors());
    }

    public ThreadPool(int numOfThreads){
        if(numOfThreads <= 0){
            throw new IllegalArgumentException("Illegal numOfThreads");
        }
        this.numOfThreads.set(numOfThreads);
        createThreads(numOfThreads);
    }

    private Future<Void> submit (Runnable request , Priority priority){
        return submit(Executors.callable(request, null), priority);
    }
    public <T> Future<T> submit (Runnable request , T result , Priority priority){
        return submit(Executors.callable(request, result), priority);
    }
    public <T> Future<T> submit (Callable<T> request, Priority priority){
        if(isShutdown){
            throw new RejectedExecutionException("Threapool state: shut down");
        }
        if(null == request){
            throw new IllegalArgumentException("Illegal request");
        }

        Task<T> task = new Task<>(request, priority.ordinal());
        taskWaitablePQ.enqueue(task);
        return task.future;
    }
    public <T> Future<T> submit (Callable<T> request){
        return submit (request , Priority.MEDIUM);
    }

    @Override
    public void execute(Runnable request){
        submit(Executors.callable(request, null), Priority.MEDIUM);
    }

    public void pause() {
        for (int i = 0; i < numOfThreads.get(); ++i) {
            Task<Object> pauseTask = new Task<>(pausingApple, HIGHEST_PRIORITY);
            taskWaitablePQ.enqueue(pauseTask);
        }
        isPaused = true;
    }

    public void resume(){
        pauseSemaphore.release(numOfThreads.get());
        isPaused = false;
    }

    public void shutDown(){
        isShutdown = true;
        resume();
        for (int i = 0; i < numOfThreads.get(); i++) {
            Task<Object> killTask = new Task<>(killingApple, LOWEST_PRIORITY);
            taskWaitablePQ.enqueue(killTask);
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        if(timeout <= 0){
            throw new IllegalArgumentException("Illegal timeout");
        }
        terminateSemaphore.drainPermits();
        return terminateSemaphore.tryAcquire(numOfThreads.get(),timeout,unit);
    }

    public void setNumOfThreads(int newNumOfThreads){
        if(numOfThreads.get() <= 0){
            throw new IllegalArgumentException("Illegal numOfThreads");
        }
        if(isPaused || isShutdown){
                throw new RejectedExecutionException("Threapool state: paused or shutdown");
        }
        if(numOfThreads.get() < newNumOfThreads){
            createThreads(newNumOfThreads - numOfThreads.get());
            this.numOfThreads.set(newNumOfThreads);
        } else if(numOfThreads.get() > newNumOfThreads) {
            for(int i = 0; i < numOfThreads.get() - newNumOfThreads; ++i){
                Task<Object> killTask = new Task<>(killingApple, HIGHEST_PRIORITY);
                taskWaitablePQ.enqueue(killTask);
            }
        }
    }
    /*********************************************private methods******************************************************/
    private static int calcAvailableProcessors(){
        return Runtime.getRuntime().availableProcessors();
    }
    private void createThreads(int numOfThreads){
        for(int i = 0; i < numOfThreads; ++i){
            WorkingThread worker = new WorkingThread();
            worker.start();
        }
    }
    /***************************************private class - WorkingThread********************************************/

    private class WorkingThread extends Thread {
        private volatile boolean isRunning = true;

        @Override
        public void run() {
            try{
                while (isRunning) {
                    Task<?> task = taskWaitablePQ.dequeue();
                    task.run();
                }
            }finally{
                numOfThreads.decrementAndGet();
                terminateSemaphore.release();
            }
        }
    }
    /*****************************************inner class - Task***********************************************/

    private class Task<T> implements Runnable, Comparable<Task<?>> {
        private TPFuture future = new TPFuture();
        private Callable<T> callable;
        private int priority;

        //Ctor
        public Task(Callable<T> callable, int priority){
            this.callable = callable;
            this.priority = priority;
        }

        @Override
        public int compareTo(Task<?> other){
             return this.priority - other.priority;
        }

        @Override
        public void run(){
            try {
                T computation = callable.call();
                future.setValue(computation);
            } catch (Exception e) {
                e.printStackTrace();
                future.setException(e);
            }finally{
                future.setIsDone();
            }
        }
        /*************************************** Task inner class - TPFuture ********************************************/

        private class TPFuture implements Future<T>{
            private AtomicReference<T> value = new AtomicReference<>();
            private AtomicReference<Throwable> exception = new AtomicReference<>();
            private volatile boolean isCancel = false;
            private volatile boolean isDone = false;
            private final Semaphore futureGetSem = new Semaphore(0);



            private void setValue(T val){
                value.set(val);
            }

            private void setException(Throwable throwable){
                exception.set(throwable);
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning){
                if(isDone || isCancel){
                    return false;
                }
                boolean removed = taskWaitablePQ.remove(Task.this);
                if(removed){
                    isCancel = true;
                }
                return removed;
            }

            @Override
            public boolean isCancelled(){
                return isCancel;
            }

            @Override
            public T get() throws InterruptedException, ExecutionException{
                try {
                    return get(Long.MAX_VALUE, DAYS);
                } catch (TimeoutException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException{
                if(isCancelled()){
                    throw new CancellationException("task was cancelled");
                }

                if (exception.get() != null) {
                    throw new ExecutionException(exception.get());
                }

                boolean acquired = futureGetSem.tryAcquire(timeout, unit);

                if (!acquired) {
                    throw new TimeoutException("timeout: couldn't get computation");
                }

                return value.get();
            }

            @Override
            public boolean isDone(){
                return isDone;
            }

            private void setIsDone(){
                isDone = true;
                futureGetSem.release();
            }
        } ///end of inner class TPFuture
    } ///end of inner class Task
    /*************************************** TP inner class - TPComparator ********************************************/

    private class TPComparator implements Comparator<Task<?>> {
        @Override
        public int compare(Task<?> task1, Task<?> task2) {
            return task2.compareTo(task1);
        }
    }
} ///end of class ThreadPool










