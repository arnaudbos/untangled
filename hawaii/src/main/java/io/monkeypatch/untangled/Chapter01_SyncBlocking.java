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

public class Chapter01_SyncBlocking {

    private static final int MAX_CLIENTS = 50;

    private final CoordinatorService coordinator = new CoordinatorService();
    private final GatewayService gateway = new GatewayService();

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
        Thread.sleep(5_000L);

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

class CoordinatorService {
    private static final Random random = new Random();

    Connection requestConnection(String token) {
        println("requestConnection(String token)");

        ignoreResponse(() -> blockingRequest("https://search.yahoo.com/search?q=" + (token == null ? "nothing" : token)));

        int attempt = token == null ? 0 : Integer.parseInt(token);
        return attempt > 4
            ? new Connection.Available("Ahoy!")
            : new Connection.Unavailable(20_000L, random.nextInt(2_000), String.valueOf(attempt + 1));
    }

    Connection heartbeat(String token) {
        println("heartbeat(String token)");

        ignoreResponse(() -> blockingRequest("https://search.yahoo.com/search?q=" + token));

        return new Connection.Available("Ahoy!");
    }
}

class GatewayService {
    InputStream downloadThingy() throws IOException {
        return blockingRequest("http://www.ovh.net/files/10Mio.dat");
    }
}