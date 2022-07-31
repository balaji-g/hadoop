package org.apache.hadoop.fs.s3a.cache;

import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.http.conn.HttpClientConnectionManager;

/**
 * Daemon thread to periodically check connection pools for idle connections.
 * <p>
 * Connections sitting around idle in the HTTP connection pool for too long will
 * eventually be terminated by the HyperStore end of the connection, and will go into
 * CLOSE_WAIT. If this happens, sockets will sit around in CLOSE_WAIT, still
 * using resources on the client side to manage that socket. Many sockets stuck
 * in CLOSE_WAIT can prevent the OS from creating new connections.
 * <p>
 * This class closes idle connections before they can move into the CLOSE_WAIT
 * state.
 * <p>
 */
public final class IdleConnectionReaper extends Thread {

    /** The period between invocations of the idle connection reaper. */
    private static final int PERIOD_MILLISECONDS = 1000 * 60 * 1;
    private static final int IDLE = 30;

    private static final Logger LOG =
        LoggerFactory.getLogger(IdleConnectionReaper.class);
    private List<HttpClientConnectionManager> connectionManager = new ArrayList<HttpClientConnectionManager>();

    /** Shared log for any errors during connection reaping. */

    /** Private constructor - singleton pattern. */
    private IdleConnectionReaper() {
        super("HyperStore-connection-reaper");
        setDaemon(true);
        start();
    }

    public static IdleConnectionReaper getInstance() {
        return SingletonHolder.INSTANCE;
    }

    public void registerConnectionManager(final HttpClientConnectionManager cm) {
        LOG.info("Registering ClientConnectionManager for reaping.");
        connectionManager.add(cm);
    }

    private static class SingletonHolder {
        private static final IdleConnectionReaper INSTANCE = new IdleConnectionReaper();
    }
   @Override
    public void run() {
        while (true) {

            //I don't think we need to synchronize anything here. 
            //Just one connection manager to deal with. 

            try {
                Thread.sleep(PERIOD_MILLISECONDS);
                for (final HttpClientConnectionManager cMgr : connectionManager) {
                    cMgr.closeIdleConnections(IDLE, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Shuts down the thread, allowing the class and instance to be collected.
     *
     * TODO: Is this necessary for us? 
     */
    public void shutdown() {
        /*
         * TODO: When we shutdown server, jvm is gone. So is it helpful
         * to explicitly stop the thread? 
        if (instance != null) {
            instance.interrupt();
            instance = null;
        }
        */
    }
}
