package ThreadSafePQ;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class WaitablePQueueCV<E> {
    private final PriorityQueue<E> queue;
    private Condition blockCond;
    private ReentrantLock pqLock = new ReentrantLock();

    //Ctor

    public WaitablePQueueCV(Comparator<? super E> comparator) {
        queue = new PriorityQueue<>(comparator);
        blockCond = pqLock.newCondition();
    }

    public void enqueue(E element) {
        if (element == null) {
            throw new NullPointerException();
        }
        pqLock.lock();
        try {
            queue.add(element);
//            System.out.println("Produced: " + element);
            blockCond.signalAll();
        } finally {
            pqLock.unlock();
        }
    }

    public E dequeue() {
        E data = null;
        pqLock.lock();
        try {
            while (queue.isEmpty()) {
                blockCond.await();
            }
            data = queue.poll();
//            System.out.println("Consumed: " + data);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            pqLock.unlock();
        }
        return data;
    }

    public boolean remove(E obj) {
        boolean removed= false;
        if(null == obj){
            return removed;
        }
        pqLock.lock();
        try {
           return queue.remove(obj);
        } finally {
            pqLock.unlock();
        }
    }

    //cannot promise thread safety
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    //cannot promise thread safety
    public int size() {
        return queue.size();
    }
}
