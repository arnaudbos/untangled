package io.monkeypatch.untangled;

import io.monkeypatch.untangled.utils.Connection;
import io.monkeypatch.untangled.utils.EtaExceededException;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import static io.monkeypatch.untangled.utils.IO.MAX_ETA_MS;
import static io.monkeypatch.untangled.utils.IO.ignoreContent;
import static io.monkeypatch.untangled.utils.Log.err;
import static io.monkeypatch.untangled.utils.Log.println;
import static java.lang.FiberScope.Option.CANCEL_AT_CLOSE;
import static java.lang.FiberScope.Option.PROPAGATE_CANCEL;

public class Chapter06_Structured {

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

    //<editor-fold desc="Main 'controller' function (getThingy): scoped download & heartbeats">
    private void getThingy(int i) throws EtaExceededException, IOException {
        try (FiberScope scope = FiberScope.open(CANCEL_AT_CLOSE, PROPAGATE_CANCEL)) {
            var conn = getConnection();
            println(i + " :: Got token, " + conn.getToken());

            try (InputStream content = gateway.downloadThingy()) {
                Runnable pulse = makePulse(conn, i);
                scope.schedule(pulse);

                ignoreContent(content);
            }
            println(i + " :: Download succeeded.");
        } catch (IOException e) {
            err(i + " :: Download failed.");
            throw e;
        } catch (InterruptedException e) {
            // D'oh!
            err(i + " :: Task interrupted.");
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            err(i + " :: Download failed " + sw.toString());
        }

        // Scoped fibers:
        // no "i:: Pulse!" are logged after ":: Download finished"
        // and no need to hold on to `f` to cancel it.
//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }
    //</editor-fold>

    //<editor-fold desc="Pulse: while not interrupted">
    private Runnable makePulse(Connection.Available conn, int i) {
        return () -> {
            while(!Thread.currentThread().isInterrupted()) {
                try {
                    // Periodic heartbeat
                    Thread.sleep(2_000L);

                    println(i + " :: Pulse!");
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
    private void run() throws Exception {
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
            try {
                fibers[i].join();
            } catch (Exception ignored) {
            } finally {
                println(i + " :: Download finished");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        (new Chapter06_Structured()).run();
        println("Done.");
    }
    //</editor-fold>
}
