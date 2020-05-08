package io.monkeypatch.untangled.utils;

import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.SslProvider;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.monkeypatch.untangled.utils.Log.err;
import static io.monkeypatch.untangled.utils.Log.println;

public class IO {

    public static final String HEADERS_TEMPLATE = "%s /%s HTTP/1.0\r\nAccept: %s\r\nContent-Length: %s\r\nContent-Type: text/plain\r\n";
    public static final String EMPTY = "";

    public static final long MAX_ETA_MS = 120_000;
    public static final int MAX_SIZE = 10 * 1024 * 1024;
    public static ExecutorService elasticRequestsExecutor;
    public static ExecutorService elasticServiceExecutor;
    public static ScheduledExecutorService boundedRequestsExecutor;
    public static ScheduledExecutorService boundedServiceExecutor;
    public static ScheduledExecutorService boundedPulseExecutor;


    //<editor-fold desc="synchronous thread-blocking request">
    public static void init_Chapter01_SyncBlocking() {
        elasticServiceExecutor = Executors.newCachedThreadPool(new PrefixedThreadFactory("service"));
    }

    public static InputStream blockingRequest(String url, String headers) throws IOException {
        println("Starting request to " + url);
        URL uri = new URL(url);
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
                URL uri = new URL(url);
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
                                                println("data " + data);
                                                if (handler != null) handler.received(data);
                                                attachment.flip();
                                                attachment.clear();

                                                println("read again");
                                                channel.read(attachment, attachment, this);
                                            } else if (read < 0) {
                                                try {
                                                    channel.close();
                                                } catch (IOException e) {
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
                                            } catch (IOException e) {
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
                                } catch (IOException e) {
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
                        } catch (IOException e) {
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
    private static HttpClient httpClient;
    private static Logger reactiveLogger = Loggers.getLogger("http-client");

    public static void init_Chapter04_Reactive() {
//        Loggers.useConsoleLoggers();
        httpClient = HttpClient.create(ConnectionProvider.elastic("plop"))
            .wiretap(true)
            .tcpConfiguration(tcpClient ->
                tcpClient
                    .secure(SslProvider.builder()
                        .sslContext(SslContextBuilder.forClient())
                        .defaultConfiguration(SslProvider.DefaultConfigurationType.TCP)
                        .handshakeTimeoutMillis(600_000)
                        .build())
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 300_000)
                    .doOnConnected(conn -> conn
                        .addHandlerLast(new ReadTimeoutHandler(300, TimeUnit.SECONDS))
                        .addHandlerLast(new WriteTimeoutHandler(300, TimeUnit.SECONDS)))
            );
    }

    public static Flux<byte[]> reactiveRequest(String url) {
        return Mono.fromCallable(() -> {
            println("Starting request to " + url);
            return new URL(url);
        }).flatMapMany(uri -> httpClient
            // I'm lazy and reactor-netty's client is right there on the shelf...
            .get()
            .uri(uri.toString())
            .responseContent()
            .asByteArray()
//            .doOnError(t -> err("req error " + t.getMessage()))
//            .doOnCancel(() -> err("req cancelled " + url))
//            .log(reactiveLogger)
        );
    }
    //</editor-fold>

    //<editor-fold desc="synchronous fiber-blocking (non thread-blocking) request">
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
    private static final Pattern available = Pattern.compile("(?i)^Available[(]token=(?<token>[0-9]+)[)]");
    private static final Pattern unavailable = Pattern.compile("(?i)^Unavailable[(]eta=(?<eta>[0-9]+),wait=(?<wait>[0-9]+),token=(?<token>[0-9]+)[)]");

    public static Connection parseToken(CheckedSupplier<InputStream> response) {
//        StringBuilder builder = new StringBuilder();
        try (InputStream is = response.get()) {//;
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = is.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }
            return parseConnection(result.toString(StandardCharsets.UTF_8.name()).substring(34));
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("No token");
//        }
//        try (Reader reader = new BufferedReader(new InputStreamReader
//            (response.get(), Charset.forName(StandardCharsets.UTF_8.name())))) {
//            int c = 0;
//            while ((c = reader.read()) != -1) {
//                builder.append((char) c);
//            }
//            return parseConnection(builder.toString().substring(34));
//        } catch (IOException e) {
//            // ignore: I just want latency
//            e.printStackTrace();
//
//            throw new IllegalStateException("No token");
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("No token");
        }
    }

    public static Connection parseConnection(String resp) {
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

        throw new IllegalStateException("Wrong token");
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
        } catch (IOException e) {
            // ignore: I just want latency
        }
    }
    //</editor-fold>
}
