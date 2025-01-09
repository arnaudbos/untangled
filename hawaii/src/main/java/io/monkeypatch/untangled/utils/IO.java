package io.monkeypatch.untangled.utils;

import io.netty.handler.logging.LogLevel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.TcpClient;
import reactor.netty.transport.logging.AdvancedByteBufFormat;
import reactor.util.Logger;
import reactor.util.Loggers;

import javax.net.ssl.SSLException;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.monkeypatch.untangled.utils.Log.err;
import static io.monkeypatch.untangled.utils.Log.println;

public class IO {

    public final static int DEMO_SERVER_PORT = 7001;
    public final static String DEMO_SERVER_URL = "http://192.168.86.41:" + DEMO_SERVER_PORT;

    public static final String HEADERS_TEMPLATE = "%s /%s HTTP/1.0\r\nAccept: %s\r\nContent-Length: %s\r\nContent-Type: text/plain\r\n";

    public static final long MAX_ETA_MS = 120_000;
    public static final int MAX_SIZE = 10 * 1024 * 1024;
    public static ExecutorService elasticRequestsExecutor;
    public static ExecutorService elasticServiceExecutor;
    public static ScheduledExecutorService boundedRequestsExecutor;
    public static ScheduledExecutorService boundedServiceExecutor;
    public static ScheduledExecutorService boundedPulseExecutor;
    public static ExecutorService unboundedServiceExecutor;


    //<editor-fold desc="synchronous thread-blocking request">
    public static void init_Chapter01_SyncBlocking() {
        elasticServiceExecutor = Executors.newCachedThreadPool(new PrefixedThreadFactory("service"));
    }

    public static InputStream blockingRequest(String url, String headers) throws IOException, URISyntaxException {
        println("Starting request to " + url);
        URL uri = new URI(url).toURL();
        SocketAddress serverAddress = new InetSocketAddress(uri.getHost(), uri.getPort());
        SocketChannel channel = SocketChannel.open(serverAddress);
        ByteBuffer buffer = ByteBuffer.wrap((headers + "Host: " + uri.getHost() + "\r\n\r\n").getBytes());
        do {
            channel.write(buffer);
        } while(buffer.hasRemaining());

        return channel.socket().getInputStream();
    }
    //</editor-fold>

    //<editor-fold desc="asynchronous thread-blocking request">
    public static void init_Chapter02_AsyncBlocking() {
        elasticRequestsExecutor = Executors.newCachedThreadPool(new PrefixedThreadFactory("requests"));
        elasticServiceExecutor = Executors.newCachedThreadPool(new PrefixedThreadFactory("service"));
    }

    public static void init_Chapter02bis_ScheduledFully() {
        boundedRequestsExecutor = Executors.newScheduledThreadPool(10, new PrefixedThreadFactory("requests"));
        boundedServiceExecutor = Executors.newScheduledThreadPool(10, new PrefixedThreadFactory("service"));
        boundedPulseExecutor = Executors.newScheduledThreadPool(10, new PrefixedThreadFactory("pulse"));
    }

    public static void asyncRequest(ExecutorService executor, String url, String headers, CompletionHandler<InputStream> handler) {
        executor.submit(() -> {
            try {
                InputStream is = blockingRequest(url, headers);
                if (handler!=null)
                    handler.completed(is);
            } catch (Exception e) {
                if (handler!=null)
                    handler.failed(e);
            }
        });
    }
    //</editor-fold>

    //<editor-fold desc="asynchronous non thread-blocking request">
    private static AsynchronousChannelGroup group;

    public static void init_Chapter03_AsyncNonBlocking() {
        boundedServiceExecutor = Executors.newScheduledThreadPool(10, new PrefixedThreadFactory("service"));

        try {
            group = AsynchronousChannelGroup.withThreadPool(boundedServiceExecutor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void asyncNonBlockingRequest(ExecutorService executor, String url, String headers, RequestHandler handler) {
        executor.submit(() -> {
            try {
                println("Starting request to " + url);
                URL uri = new URI(url).toURL();
                SocketAddress serverAddress = new InetSocketAddress(uri.getHost(), uri.getPort());
                AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group);

                channel.connect(serverAddress, null, new java.nio.channels.CompletionHandler<Void, Void>() {
                    @Override
                    public void completed(Void result, Void attachment) {
//                        println("Socket connect completed");
                        ByteBuffer headersBuffer = ByteBuffer.wrap((headers + "Host: " + uri.getHost() + "\r\n\r\n").getBytes());
                        ByteBuffer responseBuffer = ByteBuffer.allocate(1024);
                        channel.write(headersBuffer, headersBuffer, new java.nio.channels.CompletionHandler<>() {
                            @Override
                            public void completed(Integer written, ByteBuffer attachment) {
//                                println("Socket write completed");
                                if (attachment.hasRemaining()) {
                                    channel.write(attachment, attachment, this);
                                } else {
                                    channel.read(responseBuffer, responseBuffer, new java.nio.channels.CompletionHandler<>() {
                                        @Override
                                        public void completed(Integer read, ByteBuffer attachment) {
                                            println("Socket read completed: " + read);
                                            if (handler.isCancelled()) {
                                                println("cancelled");
                                                read = -1;
                                            }

                                            if (read > 0) {
                                                attachment.flip();
                                                byte[] data = new byte[attachment.limit()];
                                                attachment.get(data);
                                                println("data " + Arrays.toString(data));
                                                if (handler != null) handler.received(data);
                                                attachment.flip();
                                                attachment.clear();

                                                println("read again");
                                                channel.read(attachment, attachment, this);
                                            } else if (read < 0) {
                                                try {
                                                    channel.close();
                                                } catch (IOException _) {
                                                }
                                                println("complete!");
                                                if (handler != null) handler.completed();
                                            } else {
                                                println("else read");
                                                channel.read(attachment, attachment, this);
                                            }
                                        }

                                        @Override
                                        public void failed(Throwable t, ByteBuffer attachment) {
                                            err("Read failed");
                                            try {
                                                channel.close();
                                            } catch (IOException _) {
                                            }
                                            if (handler != null) handler.failed(t);
                                        }
                                    });
                                }
                            }

                            @Override
                            public void failed(Throwable t, ByteBuffer attachment) {
                                err("Write failed");
                                try {
                                    channel.close();
                                } catch (IOException _) {
                                }
                                if (handler != null) handler.failed(t);
                            }
                        });
                    }

                    @Override
                    public void failed(Throwable t, Void attachment) {
                        err("Connect failed");
                        try {
                            channel.close();
                        } catch (IOException _) {
                        }
                        if (handler != null) handler.failed(t);
                    }
                });
            } catch (Exception e){
                err("request failed");
                if (handler != null) handler.failed(e);
            }
        });
    }
    //</editor-fold>

    //<editor-fold desc="reactive (asynchronous non thread-blocking request)">
    public static HttpClient httpClient;
    public static TcpClient tcpClient;
    public static Logger reactiveLogger = Loggers.getLogger("http-client");

    public static void init_Chapter04_Reactive_Http() throws SSLException {
//        Loggers.useConsoleLoggers();
        httpClient = HttpClient
            .create(
                ConnectionProvider.builder("plop")
                    .maxConnections(Integer.MAX_VALUE)
                    .pendingAcquireTimeout(Duration.ofMillis(0))
                    .pendingAcquireMaxCount(-1)
                    .maxIdleTime(Duration.ofSeconds(200))
                    .maxLifeTime(Duration.ofSeconds(600))
                    .build()
            )
            .keepAlive(false)
            .wiretap("logger-name", LogLevel.DEBUG, AdvancedByteBufFormat.TEXTUAL)
        ;
        httpClient.warmup().block();
    }

    public static void init_Chapter04_Reactive_Tcp() throws SSLException {
        tcpClient = TcpClient
            .create()
        ;
        tcpClient.warmup().block();
    }

    public static Flux<byte[]> reactiveTcpRequest(String url, String headers) {
        return Mono.fromCallable(() -> {
                println("Starting request to " + url);
                return new URI(url).toURL();
            })
            .flatMapMany(uri ->
                tcpClient
                    .host(uri.getHost())
                    .port(uri.getPort())
                    .connect()
                    .flatMapMany(c -> c.outbound()
                        .sendString(Mono.just(headers + "Host: " + uri.getHost() + "\r\n\r\n"))
                        .then()
                        .thenMany(Flux.defer(() -> c.inbound().receive().asByteArray()))
                    )
            )
            ;
    }

    public static Flux<byte[]> reactiveHttpRequest(String url) {
        return Mono.fromCallable(() -> {
                println("Starting request to " + url);
                return url;
            })
            .flatMapMany(uri -> httpClient.get()
                .uri(uri)
                .responseContent()
                .asByteArray()
//                .doOnNext(bytes -> println("read " + bytes.length + " bytes."))
                .doOnError(t -> err("req error " + t.getClass() + " ---- " + t.getMessage()))
                .doOnCancel(() -> err("req cancelled " + url))
//                .log(reactiveLogger)
            )
            ;
    }

    //</editor-fold>

    //<editor-fold desc="synchronous fiber-blocking (non thread-blocking) request">
    public static void init_Chapter05_SyncNonBlocking() {
        unboundedServiceExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public static InputStream fakeFiberRequest(String url, String headers, long delay) throws IOException {
        try {
            Thread.sleep(3_000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return new InputStream() {
            private volatile boolean closed;

            private void ensureOpen() throws IOException {
                if (closed) {
                    throw new IOException("Stream closed");
                }
            }

            @Override
            public int available () throws IOException {
                ensureOpen();
                return 0;
            }

            @Override
            public int read() throws IOException {
                ensureOpen();
                return -1;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                Objects.checkFromIndexSize(off, len, b.length);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (len == 0) {
                    return 0;
                }
                ensureOpen();
                return -1;
            }

            @Override
            public byte[] readAllBytes() throws IOException {
                ensureOpen();
                return new byte[0];
            }

            @Override
            public int readNBytes(byte[] b, int off, int len)
                throws IOException {
                Objects.checkFromIndexSize(off, len, b.length);
                ensureOpen();
                return 0;
            }

            @Override
            public byte[] readNBytes(int len) throws IOException {
                if (len < 0) {
                    throw new IllegalArgumentException("len < 0");
                }
                ensureOpen();
                return new byte[0];
            }

            @Override
            public long skip(long n) throws IOException {
                ensureOpen();
                return 0L;
            }

            @Override
            public void skipNBytes(long n) throws IOException {
                ensureOpen();
                if (n > 0) {
                    throw new EOFException();
                }
            }

            @Override
            public long transferTo(OutputStream out) throws IOException {
                Objects.requireNonNull(out);
                ensureOpen();
                return 0L;
            }

            @Override
            public void close() throws IOException {
                closed = true;
            }
        };
    }
    //</editor-fold>

    //<editor-fold desc="InputStream utils">
    private static final Pattern available = Pattern.compile("(?is).*Available[(]token=(?<token>[0-9]+)[)]");
    private static final Pattern unavailable = Pattern.compile("(?is).*Unavailable[(]eta=(?<eta>[0-9]+),wait=(?<wait>[0-9]+),token=(?<token>[0-9]+)[)]");

    public static Connection parseToken(CheckedSupplier<InputStream> response) {
//        StringBuilder builder = new StringBuilder();
        try (InputStream is = response.get()) {//;
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = is.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }
            return parseConnection(result.toString(StandardCharsets.UTF_8).substring(34));
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("No token");
        }
    }

    public static Connection parseConnection(String resp) {
        resp = resp.trim();
        Matcher m = available.matcher(resp);
        if (m.matches()) {
//            println("token available");
            return new Connection.Available(m.group("token"));
        }
        m = unavailable.matcher(resp);
        if (m.matches()) {
//            println("token unavailable");
            return new Connection.Unavailable(
                Long.parseLong(m.group("eta")),
                Long.parseLong(m.group("wait")),
                m.group("token")
            );
        }
//        println("token did not match");

        throw new IllegalStateException("Wrong token: " + resp);
    }

    public static void ignoreContent(InputStream content) throws IOException {
        byte[] buffer = new byte[1024];
        int total = 0;
        while(true) {
            int read = content.read(buffer);
            // drop it
//            println("read " + read + " bytes.");
            if (read==-1 || (total+=read)>MAX_SIZE) break;
        }
    }

    public static void ignoreResponse(CheckedSupplier<InputStream> response) {
        try (InputStream is = response.get()) {
            ignoreContent(is);
        } catch (Exception e) {
            // ignore: I just want latency
        }
    }
    //</editor-fold>
}
