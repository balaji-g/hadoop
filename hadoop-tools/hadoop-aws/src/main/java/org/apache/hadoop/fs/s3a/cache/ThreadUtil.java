package org.apache.hadoop.fs.s3a.cache;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;


public final class ThreadUtil {
    private ThreadUtil() { };

   /**
    * Returns a new thread pool configured with the default settings.
    *
    * @return A new thread pool configured with the default settings.
    */

    public static ThreadPoolExecutor createPool(final String name,
                                                final int numThreads) {

        ThreadFactory threadFactory = new ThreadFactory() {
            private int threadCount = 1;

            public Thread newThread(final Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(name + threadCount++);
                return thread;
            }
        };
        return (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads, threadFactory);
    }
}
