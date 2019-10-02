package io.monkeypatch.untangled;

import io.monkeypatch.untangled.utils.CompletionHandler;
import io.monkeypatch.untangled.utils.Connection;
import io.monkeypatch.untangled.utils.EtaExceededException;
import io.monkeypatch.untangled.utils.IO;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.monkeypatch.untangled.utils.IO.*;
import static io.monkeypatch.untangled.utils.Log.err;
import static io.monkeypatch.untangled.utils.Log.println;

public class Chapter02_AsyncBlocking {

    private static final int MAX_CLIENTS = 200;

    private final AsyncCoordinatorService coordinator = new AsyncCoordinatorService();
    private final AsyncGatewayService gateway = new AsyncGatewayService();

    //<editor-fold desc="Token calls: looks recursive due to callbacks but is not">
    private void getConnection(CompletionHandler<Connection.Available> handler) {
        getConnection(0, 0, null, handler);
    }

    private void getConnection(long eta, long wait, String token, CompletionHandler<Connection.Available> handler) {
        if (eta > MAX_ETA_MS) {
            if (handler!=null) handler.failed(new EtaExceededException());
        }

        if (wait > 0) {
            try {
                Thread.sleep(wait);
            } catch (InterruptedException e) {
                if (handler!=null) handler.failed(e);
                Thread.currentThread().interrupt();
                return;
            }
        }

        println("Retrying download after " + wait + "ms wait.");

        coordinator.requestConnection(
            token,
            new CompletionHandler<>() {
                @Override
                public void completed(Connection c) {
                    if (c instanceof Connection.Available) {
                        if (handler!=null) handler.completed((Connection.Available) c);
                    } else {
                        Connection.Unavailable unavail = (Connection.Unavailable) c;
                        getConnection(
                            unavail.getEta(),
                            unavail.getWait(),
                            unavail.getToken(),
                            handler);
                    }
                }

                @Override
                public void failed(Throwable t) {
                    if (handler!=null) handler.failed(t);
                }
            },
            elasticServiceExecutor);
    }
    //</editor-fold>

    //<editor-fold desc="Main 'controller' function (getThingy): callback hell">
    private void getThingy(int i, CompletionHandler<Void> handler) {
        println("Start getThingy.");

        getConnection(new CompletionHandler<>() {
            @Override
            public void completed(Connection.Available conn) {
                println("Got token, " + conn.getToken());

                println(i + " :: Starting thingy");
                CompletableFuture<Void> downloadFut = new CompletableFuture<>();
                gateway.downloadThingy(new CompletionHandler<>() {
                    @Override
                    public void completed(InputStream content) {
                        Thread pulse = makePulse(downloadFut, conn);
                        int total = 0;
                        try(content) {
                            println(i + " :: Starting pulse ");
                            pulse.start();

                            // Get read=-1 quickly and not all content because of HTTP 1.1 but really don't care
                            byte[] buffer = new byte[8192];
                            while(true) {
                                int read = content.read(buffer);
                                // drop it
//                                println(i + " :: Read " + read + " :: Total " + total + " :: MAX_SIZE " + MAX_SIZE + " :: " + new String(buffer));
                                if (read==-1 || (total+=read)>=MAX_SIZE) break;
                            }

                            println("Download finished");

                            if (handler!=null)
                                handler.completed(null);
                        } catch (FileNotFoundException e) {
                            err("Couldn't write to temp file.");
                            if (handler!=null)
                                handler.failed(e);
                        } catch (IOException e) {
                            err("Download failed.");
                            if (handler!=null)
                                handler.failed(e);
                        } finally {
                            downloadFut.complete(null);
                        }
                    }

                    @Override
                    public void failed(Throwable t) {
                        if (t instanceof EtaExceededException) {
                            err("Couldn't getThingy because ETA exceeded: " + t);
                        } else {
                            err("Couldn't getThingy because something failed: " + t);
                        }
                        if (handler!=null) handler.failed(t);
                    }
                }, elasticServiceExecutor);

            }

            @Override
            public void failed(Throwable t) {
                err("Task failed.");
                if (handler!=null) handler.failed(t);
            }
        });
    }
    //</editor-fold>

    //<editor-fold desc="Pulse: need a reference to the download future">
    private Thread makePulse(Future<Void> download, Connection.Available conn) {
        return new Thread(() -> {
            while(!Thread.currentThread().isInterrupted() && !download.isDone()) {
                try {
                    // Periodic heartbeat
                    Thread.sleep(2_000L);

                    println("Pulse!");
                    coordinator.heartbeat(
                        conn.getToken(),
                        null,
                        null
                    );
                } catch (InterruptedException e) {
                    // D'oh!
                    Thread.currentThread().interrupt();
                }
            }
        });
    }
    //</editor-fold>

    //<editor-fold desc="Run: simulate client calls">
    private void run() throws InterruptedException, ExecutionException {
        Thread.sleep(15_000L);

        CompletableFuture<Void>[] futures = new CompletableFuture[MAX_CLIENTS];
        for(int i=0; i<MAX_CLIENTS; i++) {
            int finalI = i;
            futures[i] = new CompletableFuture<>();
            getThingy(finalI, new CompletionHandler<>() {
                @Override
                public void completed(Void result) {
                    futures[finalI].complete(result);
                }

                @Override
                public void failed(Throwable t) {
                    futures[finalI].completeExceptionally(t);
                }
            });
        }

        for(int i=0; i<MAX_CLIENTS; i++) {
            futures[i].get();
        }

        elasticRequestsExecutor.shutdown();
        elasticServiceExecutor.shutdown();
        while (!elasticRequestsExecutor.isTerminated() || !elasticServiceExecutor.isTerminated()) {
            Thread.sleep(2_000L);
        }
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        IO.init_Chapter02_AsyncBlocking();
        (new Chapter02_AsyncBlocking()).run();
        println("Done.");
    }
    //</editor-fold>
}


class AsyncCoordinatorService {
    private static final Random random = new Random();

    void requestConnection(String token, CompletionHandler<Connection> handler, ExecutorService handlerExecutor) {
        println("requestConnection(String token)");

        asyncRequest(
            elasticServiceExecutor,
            "http://localhost:7000",
            String.format(HEADERS_TEMPLATE, "GET", "token?value=" + (token == null ? "nothing" : token), "text/*", String.valueOf(0)),
            new CompletionHandler<>() {
                @Override
                public void completed(InputStream is) {
                    Runnable r = () -> {
                        if (handler != null)
                            handler.completed(parseToken(() -> is));
                    };
                    if (handlerExecutor!=null) {
                        handlerExecutor.submit(r);
                    } else {
                        r.run();
                    }
                }

                @Override
                public void failed(Throwable t) {
                    if (handler != null)
                        if (handlerExecutor!=null) {
                            handlerExecutor.submit(() -> handler.failed(t));
                        } else {
                            handler.failed(t);
                        }
                }
            });
    }

    void heartbeat(String token, CompletionHandler<Connection> handler, ExecutorService handlerExecutor) {
        println("heartbeat(String token)");

        asyncRequest(
            elasticServiceExecutor,
            "http://localhost:7000",
            String.format(HEADERS_TEMPLATE, "GET", "heartbeat?token=" + token, "text/*", String.valueOf(0)),
            new CompletionHandler<>() {
                @Override
                public void completed(InputStream is) {
                    Runnable r = () -> {
                        if (handler != null)
                            handler.completed(parseToken(() -> is));
                    };
                    if (handlerExecutor!=null) {
                        handlerExecutor.submit(r);
                    } else {
                        r.run();
                    }
                }

                @Override
                public void failed(Throwable t) {
                    if (handler != null)
                        if (handlerExecutor!=null) {
                            handlerExecutor.submit(() -> handler.failed(t));
                        } else {
                            handler.failed(t);
                        }
                }
            });
    }
}

class AsyncGatewayService {
    void downloadThingy(CompletionHandler<InputStream> handler, ExecutorService handlerExecutor) {
        asyncRequest(
            elasticRequestsExecutor,
            "http://localhost:7000",
            String.format(HEADERS_TEMPLATE, "GET", "download", "text/*", String.valueOf(0)),
            new CompletionHandler<>() {
                @Override
                public void completed(InputStream result) {
                    if (handler != null)
                        if (handlerExecutor!=null) {
                            handlerExecutor.submit(() -> handler.completed(result));
                        } else {
                            handler.completed(result);
                        }
                }

                @Override
                public void failed(Throwable t) {
                    if (handler != null)
                        if (handlerExecutor!=null) {
                            handlerExecutor.submit(() -> handler.failed(t));
                        } else {
                            handler.failed(t);
                        }
                }
            });
    }
}