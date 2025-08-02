package ThreadSafePQ;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

public class WaitablePQueueSem<E> {
    private final PriorityQueue<E> queue;
    private Semaphore dequeueSem = new Semaphore(0);
    private ReentrantLock pqLock = new ReentrantLock();

    //Ctor
    public WaitablePQueueSem(Comparator<? super E> comparator) {
        if (comparator == null) {
            throw new NullPointerException("invalid comperator");
        }
        queue = new PriorityQueue<>(comparator);
    }

    public void enqueue(E element) {
        if (element == null) {
            throw new NullPointerException("null element");
        }
        pqLock.lock();
            queue.add(element);
//            System.out.println("Produced: " + element);
        pqLock.unlock();
        dequeueSem.release();
    }

    public E dequeue() throws InterruptedException {
        E data = null;
        dequeueSem.acquire();
        pqLock.lock();
            data = queue.poll();
//                System.out.println("Consumed: " + data);
        pqLock.unlock();
        return data;
    }

    public boolean remove(E obj) {
        boolean removed = false;
        if (null == obj) {
            return removed;
        }

        boolean acquired = dequeueSem.tryAcquire();

        pqLock.lock();
            if (acquired) {
                removed = queue.remove(obj);
            }
        pqLock.unlock();
        if (!removed) {
            dequeueSem.release();
        }
        return removed;
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
