package il.co.ilrd.ThreadPool;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ThreadPoolTest {

    private static AtomicInteger counter = new AtomicInteger(0);

    private Callable<String> genericRequest() {
        return new Callable<String>() {
            @Override
            public String call() throws Exception {
                Thread.sleep(5);
                return Thread.currentThread().getName() + ", Priority:MEDIUM";
            }
        };
    }

    private Runnable genericRunnable() {
        return new Runnable() {
            @Override
            public void run(){
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.out.println(Thread.currentThread().getName());
            }
        };
    }
    private Callable<String> printRequestWithCounter(){
        return new Callable<String>() {
            @Override
            public String call() throws Exception {
                Thread.currentThread().sleep(5);
                System.out.println(Thread.currentThread().getName() + ", Counter: " + counter.addAndGet(1));
                return Thread.currentThread().getName();
            }
        };
    }

    private Callable<String> genericMediumPrintRequest() {
        return new Callable<String>() {
            @Override
            public String call() throws Exception {
                Thread.currentThread().sleep(5);
                System.out.println(Thread.currentThread().getName() + ", Priority:MEDIUM");
                return Thread.currentThread().getName();
            }
        };
    }

    private Callable<String> genericHighPrintRequest() {
        return new Callable<String>() {
            @Override
            public String call() throws Exception {
                Thread.currentThread().sleep(5);
                System.out.println(Thread.currentThread().getName() + ", Priority:HIGH");
                return Thread.currentThread().getName();
            }
        };
    }

    private Callable<String> genericLowPrintRequest() {
        return new Callable<String>() {
            @Override
            public String call() throws Exception {
                Thread.currentThread().sleep(5);
                System.out.println(Thread.currentThread().getName() + ", Priority:LOW");
                return Thread.currentThread().getName();
            }
        };
    }

    private Callable<String> highPriorityRequest() {
        return new Callable<String>() {
            @Override
            public String call() throws Exception {
                return Thread.currentThread().getName() + ", Priority:HIGH";
            }
        };
    }

    @Test
    @DisplayName("simple single submit")
    void singleSubmit() throws ExecutionException, InterruptedException {
        ThreadPool myThreadPool =  new ThreadPool(3);
        Future<String> future = myThreadPool.submit(genericRequest());
        String result = future.get();
        System.out.println(result);
    }

    @Test
    @DisplayName("future get methods with simple single submit")
    void testFutureGet() throws ExecutionException, InterruptedException, TimeoutException {
        ThreadPool myThreadPool =  new ThreadPool(3);
        Future<String> future = myThreadPool.submit(genericRequest());
        String result = future.get();
        System.out.println(result);
        future = myThreadPool.submit(genericRequest());
        result = future.get(5, TimeUnit.SECONDS);
        System.out.println(result);
    }

    @Test
    @DisplayName("many submit")
    void manySubmit() throws ExecutionException, InterruptedException {
        ThreadPool myThreadPool =  new ThreadPool(3);

        Future<String> future1 = myThreadPool.submit(genericRequest());
        Future<String> future2 = myThreadPool.submit(genericRequest());
        Future<String> future3 = myThreadPool.submit(genericRequest());
        Future<String> future4 = myThreadPool.submit(genericRequest());
        Future<String> future5 = myThreadPool.submit(genericRequest());

        Thread.sleep(1000);

        String result1 = future1.get();
        String result2 = future2.get();
        String result3 = future3.get();
        String result4 = future4.get();
        String result5 = future5.get();

        System.out.println(result1);
        System.out.println(result2);
        System.out.println(result3);
        System.out.println(result4);
        System.out.println(result5);

        Thread.sleep(10000);

    }

    @Test
    @DisplayName("high priority submit")
    void testSubmit1() throws ExecutionException, InterruptedException {
        ThreadPool myThreadPool =  new ThreadPool(1);
        Future<String> future1 = myThreadPool.submit(genericRequest(), ThreadPool.Priority.LOW);
        Future<String> future2 = myThreadPool.submit(genericRequest());
        Future<String> future3 = myThreadPool.submit(genericRequest());
        Future<String> future4 = myThreadPool.submit(genericRequest(), ThreadPool.Priority.LOW);
        Future<String> future5 = myThreadPool.submit(genericRequest());
        Future<String> future6 = myThreadPool.submit(highPriorityRequest(), ThreadPool.Priority.HIGH);
        Future<String> future7 = myThreadPool.submit(genericRequest());
        Future<String> future8 = myThreadPool.submit(genericRequest());
        Future<String> future9 = myThreadPool.submit(genericRequest());
        Future<String> future10 = myThreadPool.submit(highPriorityRequest(), ThreadPool.Priority.HIGH);

        String result1 = future1.get();
        String result2 = future2.get();
        String result3 = future3.get();
        String result4 = future4.get();
        String result5 = future5.get();
        String result6 = future6.get();
        String result7 = future7.get();
        String result8 = future8.get();
        String result9 = future9.get();
        String result10 = future10.get();

        System.out.println(result1);
        System.out.println(result2);
        System.out.println(result3);
        System.out.println(result4);
        System.out.println(result5);
        System.out.println(result6);
        System.out.println(result7);
        System.out.println(result8);
        System.out.println(result9);
        System.out.println(result10);

        Thread.sleep(10000);
    }

    @Test
    @DisplayName("compareTo - priority test")
    void testSubmit2() throws ExecutionException, InterruptedException {
        ThreadPool myThreadPool =  new ThreadPool(2);
        myThreadPool.pause();
        Thread.sleep(2);
        myThreadPool.submit(genericLowPrintRequest(), ThreadPool.Priority.LOW);
        myThreadPool.submit(genericMediumPrintRequest());
        myThreadPool.submit(genericMediumPrintRequest());
        myThreadPool.submit(genericLowPrintRequest(), ThreadPool.Priority.LOW);
        myThreadPool.submit(genericMediumPrintRequest());
        myThreadPool.submit(genericHighPrintRequest(), ThreadPool.Priority.HIGH);
        myThreadPool.submit(genericMediumPrintRequest());
        myThreadPool.submit(genericMediumPrintRequest());
        myThreadPool.submit(genericMediumPrintRequest());
        myThreadPool.submit(genericHighPrintRequest(), ThreadPool.Priority.HIGH);

        Thread.sleep(1000);
        myThreadPool.resume();
        myThreadPool.shutDown();
        myThreadPool.awaitTermination(10, TimeUnit.SECONDS);

    }

    @Test
    @DisplayName("sending a runnable")
    void testRunnable() throws InterruptedException, ExecutionException {
        ThreadPool myThreadPool =  new ThreadPool(3);

        String result = "Task is done";
        Future<String> future = myThreadPool.submit(genericRunnable(), result, ThreadPool.Priority.MEDIUM);

        String finalResult = future.get();

        System.out.println(finalResult);

    }

    @Test
    @DisplayName("execute test")
    void execute() throws InterruptedException {
        ThreadPool myThreadPool =  new ThreadPool(3);
        myThreadPool.execute(genericRunnable());
        Thread.sleep(300);
    }


    @Test
    @DisplayName("shutdown exception test")
    public void testExceptionIsThrown() {
        assertThrows(RejectedExecutionException.class, this::throwException);
    }

    private void throwException() {
        ThreadPool myThreadPool =  new ThreadPool(1);
        myThreadPool.shutDown();
        myThreadPool.submit(genericRequest());
    }


    @Test
    @DisplayName("all requests are executed after shutdown requested")
    public void testShutdown() throws InterruptedException {

        ThreadPool myThreadPool =  new ThreadPool(1);

        for(int i = 0; i < 100; ++i){
            myThreadPool.submit(printRequestWithCounter());
        }
        myThreadPool.shutDown();
        boolean isDone = myThreadPool.awaitTermination(10, TimeUnit.SECONDS);
        assertTrue(isDone);

        Thread.sleep(1000); // junit thread sleep
    }

    @Test
    @DisplayName("pause on empty priorityQueue and submit requests")
    void pause(){
        ThreadPool myThreadPool =  new ThreadPool(3);
        myThreadPool.pause();
        myThreadPool.submit(genericMediumPrintRequest());
        myThreadPool.submit(genericMediumPrintRequest());
        myThreadPool.submit(genericMediumPrintRequest());
        myThreadPool.submit(genericMediumPrintRequest());
        myThreadPool.shutDown();
    }

    @Test
    @DisplayName("pause on empty priorityQueue, submit requests and resume")
    void resume() throws InterruptedException {
        ThreadPool myThreadPool =  new ThreadPool(2);
        myThreadPool.pause();
        Thread.sleep(20);
        myThreadPool.submit(genericMediumPrintRequest());
        myThreadPool.submit(genericMediumPrintRequest());
        myThreadPool.submit(genericMediumPrintRequest());
        myThreadPool.submit(genericMediumPrintRequest());
        myThreadPool.resume();
        Thread.sleep(1000);
    }

    @Test
    @DisplayName("check await termination")
    void awaitTermination() throws InterruptedException {
        ThreadPool myThreadPool =  new ThreadPool(1);
        for(int i = 0; i < 1000000; ++i){
            myThreadPool.submit(printRequestWithCounter());
        }
        myThreadPool.shutDown();
        boolean isDead = myThreadPool.awaitTermination(2, TimeUnit.MILLISECONDS);
        System.out.println("Did all tasks finished after shutdown? " + isDead);
        Thread.sleep(1000);
    }

    @Test
    @DisplayName("check await termination")
    void awaitTermination2() throws InterruptedException {
        ThreadPool myThreadPool =  new ThreadPool(1);
        for(int i = 0; i < 10; ++i){
            myThreadPool.submit(printRequestWithCounter());
        }
        myThreadPool.shutDown();
        boolean isDead = myThreadPool.awaitTermination(2, TimeUnit.SECONDS);
        Thread.sleep(30); // junit thread sleep
        System.out.println("Did all tasks finished after shutdown? " + isDead);
    }

    @Test
    @DisplayName("check adding threads")
    void setNumOfThreads() throws InterruptedException {
        ThreadPool myThreadPool =  new ThreadPool(2);
        for(int i = 0; i < 1000; ++i){
            myThreadPool.submit(printRequestWithCounter());
        }
        Thread.sleep(10);
        myThreadPool.setNumOfThreads(4);
        myThreadPool.shutDown();
        boolean isFinished = myThreadPool.awaitTermination(60, TimeUnit.SECONDS);
        assertTrue(isFinished);
        Thread.sleep(10000);
    }

    @Test
    @DisplayName("check removing threads")
    void removeThreads() throws InterruptedException {
        ThreadPool myThreadPool =  new ThreadPool(4);
        for(int i = 0; i < 1000; ++i){
            myThreadPool.submit(printRequestWithCounter());
        }
        Thread.sleep(10);
        myThreadPool.setNumOfThreads(1);
        myThreadPool.shutDown();
        boolean isFinished = myThreadPool.awaitTermination(60, TimeUnit.SECONDS);
        assertTrue(isFinished);
        Thread.sleep(10000);
    }

    @Test
    @DisplayName("test for future cancel and isCancel")
    void testFutureCancel() throws InterruptedException {
        ThreadPool pool = new ThreadPool(1);
        pool.pause();
        Future<String> future = pool.submit(printRequestWithCounter());
        boolean cancelRes = future.cancel(false);
        assertTrue(cancelRes, "task should be cancelled successfully ");
        assertTrue(future.isCancelled());
        boolean secondCancelResult = future.cancel(false);
        assertFalse(secondCancelResult, "second call, should return false");
        pool.shutDown();
        Thread.sleep(1000);
    }
}