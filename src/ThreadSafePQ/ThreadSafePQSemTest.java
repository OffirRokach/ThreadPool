package ThreadSafePQ;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;
class WaitablePQueueSemTest {

    private WaitablePQueueSem<Integer> queue;
    final int NUM_OF_PRODUCERS = 2;
    final int NUM_OF_CONSUMERS = 3;
    final int NUM_OF_REMOVERS = 2;
    final int WORK_PER_PRODUCER = 10;

    @BeforeEach
    void setUp() throws InterruptedException {
        Comparator<Integer> integerComparator = (num1, num2) -> num1-num2;
        queue = new WaitablePQueueSem<>(integerComparator);
    }

    @Test
    @DisplayName("synchronized enqueue")
    void enqueue() throws InterruptedException {

        Thread[] enqueueThreads = new Thread[NUM_OF_PRODUCERS];

        for (int i = 0; i < NUM_OF_PRODUCERS; i++) {
            int baseValue = i * WORK_PER_PRODUCER;
            enqueueThreads[i] = new Thread(() -> {
                for (int j = baseValue; j < baseValue + WORK_PER_PRODUCER; ++j) {
                    queue.enqueue(j);
                }
            });
            enqueueThreads[i].start();
        }

        for (Thread producer : enqueueThreads) {
            producer.join();
        }

        int previous = queue.dequeue();
        for (int i = 1; i < NUM_OF_PRODUCERS * WORK_PER_PRODUCER; ++i) {
            int current = queue.dequeue();
            assertTrue(previous <= current, "Priority queue order violation");
            previous = current;
        }

        for (Thread producer : enqueueThreads) {
            producer.interrupt();
        }
    }

    @Test
    @DisplayName("dequeue method blocking")
    void dequeue() throws InterruptedException {

        assertTrue(queue.isEmpty());

        final int[] result = {0};
        Thread dequeueThread = new Thread(() -> {
            try {
                result[0] = queue.dequeue();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        dequeueThread.start();
        Thread.sleep(10);
        assertTrue(dequeueThread.getState() == Thread.State.WAITING
                || dequeueThread.getState() == Thread.State.BLOCKED);
        assertTrue(dequeueThread.isAlive());
        queue.enqueue(100);
        assertFalse(queue.isEmpty());
        dequeueThread.join();
        assertFalse(dequeueThread.isAlive());
        assertEquals(100, result[0]);

    }

    @Test
    @DisplayName("Test Remove - simple")
    void testRemove(){
        WaitablePQueueSem<Integer> queue = new WaitablePQueueSem<>(Comparator.naturalOrder());

        queue.enqueue(1);
        queue.enqueue(2);
        queue.enqueue(3);

//        boolean removed = queue.remove("bar");
//        assertFalse(removed);

        boolean removed = queue.remove(8);
        assertFalse(removed);

        removed = queue.remove(2);
        assertTrue(removed);
    }

    @Test
    @DisplayName("remove - concurrency test")

    void removeConcurrency() throws InterruptedException {
        assertTrue(queue.isEmpty());
        assertEquals(0, queue.size());

        Thread[] enqueueThreads = new Thread[NUM_OF_PRODUCERS];
        Thread[] dequeueThreads = new Thread[NUM_OF_CONSUMERS];
        Thread[] removeThreads = new Thread[NUM_OF_REMOVERS];

        for (int i = 0; i < NUM_OF_PRODUCERS; i++) {
            int baseValue = i * WORK_PER_PRODUCER;
            enqueueThreads[i] = new Thread(() -> {
                for (int j = baseValue; j < baseValue + WORK_PER_PRODUCER; ++j) {
                    queue.enqueue(j);
                }
            });
            enqueueThreads[i].start();
        }

        for (int i = 0; i < NUM_OF_REMOVERS; i++) {
            removeThreads[i] = new Thread(() -> {
                    boolean res = queue.remove(12);
                    System.out.println("Remove status: " + (res ? "Success": "Element not found"));
                    res = queue.remove(0);
                    System.out.println("Remove status: " + (res ? "Success": "Element not found"));

            });
           removeThreads[i].start();
        }

        for (int i = 0; i < NUM_OF_CONSUMERS; i++) {
            dequeueThreads[i] = new Thread(() -> {
                for (int j = 0; j < WORK_PER_PRODUCER; ++j) {
                    try {
                        queue.dequeue();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            dequeueThreads[i].start();
        }

        for (Thread producer : enqueueThreads) {
            producer.join();
        }
        for (Thread consumer : dequeueThreads) {
            consumer.join();
        }
        for (Thread remover : removeThreads) {
            remover.join();
        }

        assertTrue(queue.isEmpty());
        assertEquals(0, queue.size());

    }


    @Test
    @DisplayName("isEmpty and Size")
    void isEmptyAndSize() throws InterruptedException {
        assertTrue(queue.isEmpty());
        assertEquals(0, queue.size());

        Thread[] enqueueThreads = new Thread[NUM_OF_PRODUCERS];
        Thread[] dequeueThreads = new Thread[NUM_OF_CONSUMERS];

        for (int i = 0; i < NUM_OF_PRODUCERS; i++) {
            int baseValue = i * WORK_PER_PRODUCER;
            enqueueThreads[i] = new Thread(() -> {
                for (int j = baseValue; j < baseValue + WORK_PER_PRODUCER; ++j) {
                    queue.enqueue(j);
                }
            });
            enqueueThreads[i].start();
        }

        for (int i = 0; i < NUM_OF_CONSUMERS; i++) {
            dequeueThreads[i] = new Thread(() -> {
                for (int j = 0; j < WORK_PER_PRODUCER; ++j) {
                    try {
                        queue.dequeue();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            dequeueThreads[i].start();
        }

        for (Thread producer : enqueueThreads) {
            producer.join();
        }
        for (Thread consumer : dequeueThreads) {
            consumer.join();
        }

        assertTrue(queue.isEmpty());
        assertEquals(0, queue.size());
    }

}
