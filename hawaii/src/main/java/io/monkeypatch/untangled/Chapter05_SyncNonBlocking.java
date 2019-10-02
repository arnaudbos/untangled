package io.monkeypatch.untangled;

import io.monkeypatch.untangled.utils.Connection;
import io.monkeypatch.untangled.utils.EtaExceededException;
import io.monkeypatch.untangled.utils.IO;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import static io.monkeypatch.untangled.utils.IO.*;
import static io.monkeypatch.untangled.utils.Log.err;
import static io.monkeypatch.untangled.utils.Log.println;

public class Chapter05_SyncNonBlocking {

    private static final int MAX_CLIENTS = 200;

    private final SyncCoordinatorService coordinator = new SyncCoordinatorService();
    private final SyncGatewayService gateway = new SyncGatewayService();

    //<editor-fold desc="Blocking token calls: easy for loop">
    private Connection.Available getConnection() throws EtaExceededException, InterruptedException {
        return getConnection(0, 0, null);
    }

    private Connection.Available getConnection(long eta, long wait, String token) throws EtaExceededException, InterruptedException {
        for(;;) {
            if (eta > MAX_ETA_MS) {
                throw new EtaExceededException();
            }

            if (wait > 0) {
                Thread.sleep(wait);
            }

            println("Retrying download after " + wait + "ms wait.");

            Connection c = coordinator.requestConnection(token);
            if (c instanceof Connection.Available) {
                return (Connection.Available) c;
            }
            Connection.Unavailable unavail = (Connection.Unavailable) c;
            eta = unavail.getEta();
            wait = unavail.getWait();
            token = unavail.getToken();
        }
    }
    //</editor-fold>

    //<editor-fold desc="Main 'controller' function (getThingy): easy imperative">
    private void getThingy(int i) throws EtaExceededException, IOException {
        println("Start getThingy.");

        try {
            Connection.Available conn = getConnection();
            println("Got token, " + conn.getToken());

            Runnable pulse = makePulse(conn);
            Fiber<Object> f = null;
            try (InputStream content = gateway.downloadThingy()) {
                f = FiberScope.background().schedule(pulse);

                ignoreContent(content);
            } catch (IOException e) {
                err("Download failed.");
                throw e;
            }
            finally {
                if (f!=null) {
                    f.cancel();
                }
            }
        } catch (InterruptedException e) {
            // D'oh!
            err("Task interrupted.");
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        println("Download finished");

        // Non-scoped fibers:
        // Without a ref on `f` and `f.cancel()`, some "i:: Pulse!"
        // could be logged after ":: Download finished"...
//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }
    //</editor-fold>

    //<editor-fold desc="Pulse: while not interrupted">
    private Runnable makePulse(Connection.Available conn) {
        return () -> {
            while(!Thread.currentThread().isInterrupted()) {
                try {
                    // Periodic heartbeat
                    Thread.sleep(2_000L);

                    println("Pulse!");
                    coordinator.heartbeat(conn.getToken());
                } catch (InterruptedException e) {
                    // D'oh!
                    Thread.currentThread().interrupt();
                }
            }
        };
    }
    //</editor-fold>

    //<editor-fold desc="Run: simulate client calls">
    private void run() throws InterruptedException {
        Thread.sleep(15_000L);

        Fiber[] fibers = new Fiber[MAX_CLIENTS];
        for(int i=0; i<MAX_CLIENTS; i++) {
            int finalI = i;
            fibers[i] = FiberScope.background().schedule(() -> {
                try {
                    getThingy(finalI);
                } catch (EtaExceededException e) {
                    err("Couldn't getThingy because ETA exceeded: " + e);
                } catch (Exception e) {
                    err("Couldn't getThingy because something failed: " + e);
                    e.printStackTrace();
                }
            });
        }

        for(int i=0; i<MAX_CLIENTS; i++) {
            fibers[i].join();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        (new Chapter05_SyncNonBlocking()).run();
        println("Done.");
    }
    //</editor-fold>
}