package io.monkeypatch.untangled;

import io.monkeypatch.untangled.utils.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static io.monkeypatch.untangled.utils.IO.*;
import static io.monkeypatch.untangled.utils.Log.err;
import static io.monkeypatch.untangled.utils.Log.println;

public class Chapter03_AsyncNonBlocking {

    private static final int MAX_CLIENTS = 200;

    // Could have use a single executor (or a singleThreadExecutor for that matter),
    // but wanted to mimic code from AsyncBlocking class.

    private final AsyncNonBlockingCoordinatorService coordinator = new AsyncNonBlockingCoordinatorService();
    private final AsyncNonBlockingGatewayService gateway = new AsyncNonBlockingGatewayService();

    //<editor-fold desc="Token calls: callback hell.. /!\ check out requestConnection > asyncNonBlockingRequest for fun">
    private void getConnection(CompletionHandler<Connection.Available> handler) {
        getConnection(0, 0, null, handler);
    }

    private void getConnection(long eta, long wait, String token, CompletionHandler<Connection.Available> handler) {
        if (eta > MAX_ETA_MS) {
            if (handler!=null) handler.failed(new EtaExceededException());
        }

        boundedServiceExecutor.schedule(() -> {
            println("Retrying download after " + wait + "ms wait.");

            coordinator.requestConnection(
                token,
                new CompletionHandler<>() {
                    @Override
                    public void completed(Connection c) {
//                        println("getConnection2 completed");
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
                        err("getConnection2 failed");
                        if (handler!=null) handler.failed(t);
                    }
                }, boundedServiceExecutor);
        }, wait, TimeUnit.MILLISECONDS);
    }
    //</editor-fold>

    //<editor-fold desc="Main 'controller' function (getThingy): callback hell">
    private void getThingy(int i, CompletionHandler<Void> handler) {
        println(i + " :: Start getThingy.");

        getConnection(new CompletionHandler<>() {
            @Override
            public void completed(Connection.Available conn) {
                println(i + " :: Got token, " + conn.getToken());

                CompletableFuture<Void> downloadFut = new CompletableFuture<>();
                gateway.downloadThingy(new RequestHandler() {
                    private int total = 0;
                    private boolean pulsing = false;
                    private boolean cancelled = false;

                    @Override
                    public void received(byte[] data) {
                        int read = data.length;
//                        println(i + " :: Thingy received " + read);
                        if (!pulsing) {
                            Runnable pulse = new PulseRunnable(i, downloadFut, conn);
                            println(i + " :: Starting pulse.");
                            boundedServiceExecutor.schedule(pulse, 2_000L, TimeUnit.MILLISECONDS);
                            pulsing = true;
                        }

                        // drop it
//                        println(i + " :: Read " + read + " :: Total " + total + " :: MAX_SIZE " + MAX_SIZE + " :: " + new String(data));
//                        println("read " + read + " bytes.");
                        boolean prev = cancelled;
                        if ((total += read)>=MAX_SIZE) cancelled = true;
                        if (!prev && cancelled) {
                            println("cancelled!");
                        }
                    }

                    @Override
                    public boolean isCancelled() {
                        return cancelled;
                    }

                    @Override
                    public void completed() {
                        try {
                            if (handler!=null)
                                handler.completed(null);
                        }
                        finally {
                            if (pulsing) {
                                println(i + " :: Stopping pulse ");
                                downloadFut.complete(null);
                                pulsing = false;
                            }
                        }
                    }

                    @Override
                    public void failed(Throwable t) {
                        err(i + " :: Thingy failed");
                        if (t instanceof EtaExceededException) {
                            err("Couldn't getThingy because ETA exceeded: " + t);
                        } else {
                            err("Couldn't getThingy because something failed: " + t);
                        }
                        if (pulsing) {
                            println(i + " :: Stopping pulse ");
                            downloadFut.complete(null);
                            pulsing = false;
                        }
                        if (handler!=null) handler.failed(t);
                    }
                }, boundedServiceExecutor);

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
    class PulseRunnable implements Runnable {
        private int i;
        private Future<Void> download;
        private Connection.Available conn;

        PulseRunnable(int i, Future<Void> download, Connection.Available conn) {
            this.i = i;
            this.download = download;
            this.conn = conn;
        }

        @Override
        public void run() {
            if (!download.isDone()) {
                println(i + " :: Pulse!");
                coordinator.heartbeat(
                    conn.getToken(),
                    new CompletionHandler<>() {
                        @Override
                        public void completed(Connection result) {
                            if (!download.isDone()) {
                                boundedServiceExecutor.schedule(PulseRunnable.this, 2_000L, TimeUnit.MILLISECONDS);
                            }
                        }

                        @Override
                        public void failed(Throwable t) {
                            // Nevermind
                        }
                    },
                    boundedServiceExecutor
                );
            } else {
                println(i + " :: Pulse stopped.");
            }
        }
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
                    println(finalI + " :: Download succeeded.");
                    futures[finalI].complete(result);
                }

                @Override
                public void failed(Throwable t) {
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    t.printStackTrace(pw);
                    err(finalI + " :: Download failed " + sw.toString());
                    futures[finalI].completeExceptionally(t);
                }
            });
        }

        for(int i=0; i<MAX_CLIENTS; i++) {
            try {
                futures[i].get();
            } catch (Exception ignored) {
            } finally {
                println(i + ":: Download finished");
            }
        }

        boundedServiceExecutor.shutdown();
        while (!boundedServiceExecutor.isTerminated()) {
            Thread.sleep(2_000L);
        }
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        IO.init_Chapter03_AsyncNonBlocking();
        (new Chapter03_AsyncNonBlocking()).run();
        println("Done.");
    }
    //</editor-fold>
}


class AsyncNonBlockingCoordinatorService {
    private static final Random random = new Random();

    void requestConnection(String token, CompletionHandler<Connection> handler, ExecutorService handlerExecutor) {
        println("requestConnection(String token)");
        AtomicReference<StringBuilder> result = new AtomicReference<>(new StringBuilder());

        asyncNonBlockingRequest(boundedServiceExecutor,
            "http://localhost:7000",
            String.format(HEADERS_TEMPLATE, "GET", "token?value=" + (token == null ? "nothing" : token), "text/*", String.valueOf(0)),
            new RequestHandler() {
                @Override
                public boolean isCancelled() {
                    return false;
                }

                @Override
                public void received(byte[] data) {
                    println("requestConnection2 received " + new String(data));
                    try {
                        result.updateAndGet(sb -> sb.append(new String(data)));
//                        println("requestConnection2 read " + result.get());
                    } catch (Exception e) {
                        failed(e);
                    }
                }

                @Override
                public void completed() {
                    println("requestConnection2 completed " + result.get());
                    Runnable r = () -> {
                        if (handler != null) {
                            try {
                                Connection conn = parseConnection(result.get().toString().substring(34));
                                handler.completed(conn);
                            } catch (Exception e) {
                                failed(e);
                            }
                        }
                    };
                    if (handlerExecutor!=null) {
                        handlerExecutor.submit(r);
                    } else {
                        r.run();
                    }
                }

                @Override
                public void failed(Throwable t) {
                    err("requestConnection2 failed");
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
        AtomicReference<StringBuilder> result = new AtomicReference<>(new StringBuilder());

        asyncNonBlockingRequest(boundedServiceExecutor,
            "http://localhost:7000",
            String.format(HEADERS_TEMPLATE, "GET", "heartbeat?token=" + token, "text/*", String.valueOf(0)),
            new RequestHandler() {
                @Override
                public boolean isCancelled() {
                    return false;
                }

                @Override
                public void received(byte[] data) {
//                    println("heartbeat received");
                    Runnable r = () -> {
                        try {
                            result.updateAndGet(sb -> sb.append(new String(data)));
//                        println("heartbeat read " + result.get());
                        } catch (Exception e) {
                            failed(e);
                        }
                    };
                    if (handlerExecutor!=null) {
                        handlerExecutor.submit(r);
                    } else {
                        r.run();
                    }
                }

                @Override
                public void completed() {
//                    println("heartbeat completed");
                    Runnable r = () -> {
                        if (handler != null)
                            try {
                                Connection conn = parseConnection(result.get().toString().substring(34));
                                handler.completed(conn);
                            } catch (Exception e) {
                                failed(e);
                            }
                    };
                    if (handlerExecutor!=null) {
                        handlerExecutor.submit(r);
                    } else {
                        r.run();
                    }
                }

                @Override
                public void failed(Throwable t) {
                    err("heartbeat failed");
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

class AsyncNonBlockingGatewayService {
    void downloadThingy(RequestHandler handler, ExecutorService handlerExecutor) {
        asyncNonBlockingRequest(boundedServiceExecutor,
            "http://localhost:7000",
            String.format(HEADERS_TEMPLATE, "GET", "download", "application/octet-stream", String.valueOf(0)),
            new RequestHandler() {
                @Override
                public boolean isCancelled() {
                    return handler.isCancelled();
                }

                @Override
                public void received(byte[] data) {
//                        println("downloadThingy received");
                    if (handler != null)
                        if (handlerExecutor!=null) {
                            handlerExecutor.submit(() -> handler.received(data));
                        } else {
                            handler.received(data);
                        }
                }

                @Override
                public void completed() {
//                        println("downloadThingy completed");
                    if (handler != null)
                        if (handlerExecutor!=null) {
                            handlerExecutor.submit(handler::completed);
                        } else {
                            handler.completed();
                        }
                }

                @Override
                public void failed(Throwable t) {
                    err("downloadThingy failed");
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