package io.monkeypatch.untangled;

import io.monkeypatch.untangled.utils.Connection;
import io.monkeypatch.untangled.utils.EtaExceededException;
import io.monkeypatch.untangled.utils.IO;

import java.io.IOException;
import java.io.InputStream;

import static io.monkeypatch.untangled.utils.IO.*;
import static io.monkeypatch.untangled.utils.Log.err;
import static io.monkeypatch.untangled.utils.Log.println;

public class Chapter01_SyncBlocking {

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
    private void getThingy() throws EtaExceededException, IOException {
        println("Start getThingy.");

        try {
            Connection.Available conn = getConnection();
            println("Got token, " + conn.getToken());

            Thread pulse = makePulse(conn);
            try (InputStream content = gateway.downloadThingy()) {
                pulse.start();

                ignoreContent(content);
            } catch (IOException e) {
                err("Download failed.");
                throw e;
            }
            finally {
                pulse.interrupt();
            }
        } catch (InterruptedException e) {
            // D'oh!
            err("Task interrupted.");
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        println("Download finished");
    }
    //</editor-fold>

    //<editor-fold desc="Pulse: while not interrupted">
    private Thread makePulse(Connection.Available conn) {
        return new Thread(() -> {
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
        });
    }
    //</editor-fold>

    //<editor-fold desc="Run: simulate client calls">
    private void run() throws InterruptedException {
        Thread.sleep(15_000L);

        for(int i=0; i<MAX_CLIENTS; i++) {
            elasticServiceExecutor.submit(() -> {
                try {
                    getThingy();
                } catch (EtaExceededException e) {
                    err("Couldn't getThingy because ETA exceeded: " + e);
                } catch (Exception e) {
                    err("Couldn't getThingy because something failed: " + e);
                }
            });
        }

        elasticServiceExecutor.shutdown();
        while (!elasticServiceExecutor.isTerminated()) {
            Thread.sleep(2_000L);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        IO.init_Chapter01_SyncBlocking();
        (new Chapter01_SyncBlocking()).run();
        println("Done.");
    }
    //</editor-fold>
}

class SyncCoordinatorService {
    Connection requestConnection(String token) {
        println("requestConnection(String token)");

        return parseToken(() -> blockingRequest(
            "http://localhost:7000",
            String.format(HEADERS_TEMPLATE, "GET", "token?value=" + (token == null ? "nothing" : token), "text/*", String.valueOf(0))
        ));
    }

    Connection heartbeat(String token) {
        println("heartbeat(String token)");

        return parseToken(() -> blockingRequest(
            "https://localhost:7000",
            String.format(HEADERS_TEMPLATE, "GET", "heartbeat?token=" + token, "text/*", String.valueOf(0))
        ));
    }
}

class SyncGatewayService {
    InputStream downloadThingy() throws IOException {
        return blockingRequest(
            "http://localhost:7000",
            String.format(HEADERS_TEMPLATE, "GET", "download", "text/*", String.valueOf(0))
        );
    }
}