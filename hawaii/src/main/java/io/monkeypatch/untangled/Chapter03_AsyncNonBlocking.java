package io.monkeypatch.untangled;

import io.monkeypatch.untangled.utils.*;

import java.util.Random;
import java.util.concurrent.*;

import static io.monkeypatch.untangled.utils.IO.*;
import static io.monkeypatch.untangled.utils.Log.err;
import static io.monkeypatch.untangled.utils.Log.println;

public class Chapter03_AsyncNonBlocking {

    private static final int MAX_CLIENTS = 50;

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
        println("Start getThingy.");

        getConnection(new CompletionHandler<>() {
            @Override
            public void completed(Connection.Available conn) {
                println("Got token, " + conn.getToken());

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
                            boundedPulseExecutor.schedule(pulse, 2_000L, TimeUnit.MILLISECONDS);
                            pulsing = true;
                        }

                        // drop it
//                        println(i + " :: Read " + read + " :: Total " + total + " :: MAX_SIZE " + MAX_SIZE + " :: " + new String(data));
                        println("read " + read + " bytes.");
                        if ((total += read)>=MAX_SIZE) cancelled = true;
                    }

                    @Override
                    public boolean isCancelled() {
                        return cancelled;
                    }

                    @Override
                    public void completed() {
                        try {
                            println("Download finished");
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
                                boundedPulseExecutor.schedule(PulseRunnable.this, 2_000L, TimeUnit.MILLISECONDS);
                            }
                        }

                        @Override
                        public void failed(Throwable t) {
                            // Nevermind
                        }
                    },
                    boundedPulseExecutor
                );
            } else {
                println(i + " :: Pulse stopped.");
            }
        }
    }
    //</editor-fold>

    //<editor-fold desc="Run: simulate client calls">
    private void run() throws InterruptedException, ExecutionException {
        Thread.sleep(5_000L);

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

        boundedRequestsExecutor.shutdown();
        boundedServiceExecutor.shutdown();
        boundedPulseExecutor.shutdown();
        while (!boundedPulseExecutor.isTerminated() || !boundedServiceExecutor.isTerminated() || !boundedPulseExecutor.isTerminated()) {
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

        asyncNonBlockingRequest(boundedRequestsExecutor,
            "http://localhost:7000/token?value=" + (token == null ? "nothing" : token),
            String.format(HEADERS_TEMPLATE, "GET", EMPTY, "text/*", String.valueOf(0)),
            new RequestHandler() {
                @Override
                public boolean isCancelled() {
                    return false;
                }

                @Override
                public void received(byte[] data) {
//                        println("requestConnection2 received");
                    Runnable r = () -> {
//                            println("requestConnection2 read " + new String(data));
                    };
                    if (handlerExecutor!=null) {
                        handlerExecutor.submit(r);
                    } else {
                        r.run();
                    }
                }

                @Override
                public void completed() {
//                        println("requestConnection2 completed");
                    Runnable r = () -> {
                        int attempt = token == null ? 0 : Integer.parseInt(token);
                        if (handler != null) handler.completed(attempt > 4
                            ? new Connection.Available("Ahoy!")
                            : new Connection.Unavailable(20_000L, random.nextInt(2_000), String.valueOf(attempt + 1)));
                    };
                    if (handlerExecutor!=null) {
                        handlerExecutor.submit(r);
                    } else {
                        r.run();
                    }
                }

                @Override
                public void failed(Throwable t) {
                    err("requestConnection2 received");
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

        asyncNonBlockingRequest(boundedRequestsExecutor,
            "http://localhost:7000",
            String.format(HEADERS_TEMPLATE, "GET", "heartbeat?token=" + token, "text/*", String.valueOf(0)),
            new RequestHandler() {
                @Override
                public boolean isCancelled() {
                    return false;
                }

                @Override
                public void received(byte[] data) {
//                        println("heartbeat received");
                    Runnable r = () -> {
//                            println("heartbeat read " + new String(data));
                    };
                    if (handlerExecutor!=null) {
                        handlerExecutor.submit(r);
                    } else {
                        r.run();
                    }
                }

                @Override
                public void completed() {
//                        println("heartbeat completed");
                    Runnable r = () -> {
                        if (handler != null)
                            handler.completed(new Connection.Available("Ahoy!"));
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
        asyncNonBlockingRequest(boundedRequestsExecutor,
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
                            handlerExecutor.submit(() -> handler.completed());
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