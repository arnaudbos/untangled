package io.monkeypatch.untangled;

import io.monkeypatch.untangled.utils.CompletionHandler;
import io.monkeypatch.untangled.utils.Connection;
import io.monkeypatch.untangled.utils.EtaExceededException;
import io.monkeypatch.untangled.utils.IO;

import java.io.*;
import java.util.Random;
import java.util.concurrent.*;

import static io.monkeypatch.untangled.utils.IO.*;
import static io.monkeypatch.untangled.utils.Log.err;
import static io.monkeypatch.untangled.utils.Log.println;

public class Chapter02bis_ScheduledFully {

    private static final int MAX_CLIENTS = 200;

    private final FullyScheduledAsyncCoordinatorService coordinator = new FullyScheduledAsyncCoordinatorService();
    private final FullyScheduledAsyncGatewayService gateway = new FullyScheduledAsyncGatewayService();

    //<editor-fold desc="Token calls: looks recursive due to callbacks but is not">
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
                boundedServiceExecutor);
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

                println(i + " :: Starting thingy");
                CompletableFuture<Void> downloadFut = new CompletableFuture<>();
                gateway.downloadThingy(new CompletionHandler<>() {
                    @Override
                    public void completed(InputStream content) {
                        Runnable pulse = new PulseRunnable(i, downloadFut, conn);
                        int total = 0;
                        try(content) {
                            println(i + " :: Starting pulse ");
                            boundedPulseExecutor.schedule(pulse, 2_000L, TimeUnit.MILLISECONDS);

                            // Get read=-1 quickly and not all content because of HTTP 1.1 but really don't care
                            byte[] buffer = new byte[8192];
                            while(true) {
                                int read = content.read(buffer);
                                // drop it
//                                println(i + " :: Read " + read + " :: Total " + total + " :: MAX_SIZE " + MAX_SIZE + " :: " + new String(buffer));
                                if (read==-1 || (total+=read)>=MAX_SIZE) break;
                            }

                            if (handler!=null)
                                handler.completed(null);
                        } catch (Exception e) {
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
                }, boundedServiceExecutor);

            }

            @Override
            public void failed(Throwable t) {
                err("Download failed.");
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
                            rePulseIfNotDone();
                        }

                        @Override
                        public void failed(Throwable t) {
                            rePulseIfNotDone();
                        }
                    },
                    boundedPulseExecutor
                );
            } else {
                println(i + " :: Pulse stopped.");
            }
        }

        private void rePulseIfNotDone() {
            if (!download.isDone()) {
                boundedPulseExecutor.schedule(
                    PulseRunnable.this,
                    2_000L,
                    TimeUnit.MILLISECONDS
                );
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
                    println(finalI + " :: Download succeeded");
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

        boundedRequestsExecutor.shutdown();
        boundedServiceExecutor.shutdown();
        boundedPulseExecutor.shutdown();
        while (!boundedRequestsExecutor.isTerminated() || !boundedServiceExecutor.isTerminated() || !boundedPulseExecutor.isTerminated()) {
            Thread.sleep(2_000L);
        }
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        IO.init_Chapter02bis_ScheduledFully();
        (new Chapter02bis_ScheduledFully()).run();
        println("Done.");
    }
    //</editor-fold>
}



class FullyScheduledAsyncCoordinatorService {
    private static final Random random = new Random();

    void requestConnection(String token, CompletionHandler<Connection> handler, ExecutorService handlerExecutor) {
        println("requestConnection(String token)");

        asyncRequest(
            boundedRequestsExecutor,
            DEMO_SERVER_URL,
            String.format(HEADERS_TEMPLATE, "GET", "token?value=" + (token == null ? "nothing" : token), "text/*", String.valueOf(0)),
            new CompletionHandler<>() {
                @Override
                public void completed(InputStream is) {
                    Runnable r = () -> {
                        if (handler != null)
                            try {
                                Connection t = parseToken(() -> is);
                                handler.completed(t);
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
            boundedPulseExecutor,
            DEMO_SERVER_URL,
            String.format(HEADERS_TEMPLATE, "GET", "heartbeat?token=" + token, "text/*", String.valueOf(0)),
            new CompletionHandler<>() {
                @Override
                public void completed(InputStream is) {
                    Runnable r = () -> {
                        if (handler != null)
                            try {
                                Connection t = parseToken(() -> is);
                                handler.completed(t);
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

class FullyScheduledAsyncGatewayService {
    void downloadThingy(CompletionHandler<InputStream> handler, ExecutorService handlerExecutor) {
        asyncRequest(
            boundedRequestsExecutor,
            DEMO_SERVER_URL,
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