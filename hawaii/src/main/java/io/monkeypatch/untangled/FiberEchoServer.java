package io.monkeypatch.untangled;

import io.monkeypatch.untangled.utils.RandomGeneratedInputStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static io.monkeypatch.untangled.utils.IO.MAX_ETA_MS;
import static io.monkeypatch.untangled.utils.Log.println;

/**
 * Continuations-based TCP server stolen from Rémi Forax (https://github.com/forax/loom-fiber/blob/master/src/main/java/fr.umlv.loom/fr/umlv/loom/Server.java)
 * just tweaked a bit for routing and delaying responses via the scheduled executor.
 * The fiber version and the thread-based version (with the cached thread pool) are for experimenting.
 */
public class FiberEchoServer {

    private static final Random random = new Random();
    private static final int MAX_SIZE = 10 * 1024 * 1024;
    private static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private static final ExecutorService scheduler = Executors.newCachedThreadPool();

    static final ContinuationScope SCOPE = new ContinuationScope("server");

    public interface IO {
        int read(ByteBuffer buffer) throws IOException;
        int write(ByteBuffer buffer) throws IOException;

        default String readLine(ByteBuffer buffer) throws IOException {
            int read;
            loop: while((read = read(buffer)) != -1) {
                buffer.flip();
                while(buffer.hasRemaining()) {
                    if(buffer.get() == '\n') {
                        break loop;
                    }
                }
                if (buffer.position() == buffer.capacity()) {
                    throw new IOException("string too big");
                }
                buffer.limit(buffer.capacity());
                buffer.flip();
                buffer.clear();
            }
            if (read == -1) {
                return null;
            }
            buffer.flip();
            buffer.clear();
            byte[] array = new byte[buffer.limit() - 2];
            buffer.get(array);
            buffer.get(); // skip '\n'
            return new String(array);
        }

        default void write(String text) throws IOException {
            write(ByteBuffer.wrap(text.getBytes()));
        }
    }

    public interface IOConsumer {
        void accept(ContinuationScope scope, ByteBuffer buffer, IO io) throws IOException;
    }

    static void closeUnconditionally(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            // do nothing
        }
    }

    private static void continuationLoop(IOConsumer consumer, int localPort) throws IOException {
        System.out.println("start server on local port 7000");
        var server = ServerSocketChannel.open();
        server.configureBlocking(false);
        server.bind(new InetSocketAddress(localPort));
        var selector = Selector.open();
        var acceptContinuation = new Continuation(SCOPE, () -> {
            for (; ; ) {
                SocketChannel channel;
                try {
                    channel = server.accept();
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                    closeUnconditionally(server);
                    closeUnconditionally(selector);
                    return;
                }
                SelectionKey key;
                try {
                    channel.configureBlocking(false);
                    key = channel.register(selector, 0);
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                    closeUnconditionally(channel);
                    return;
                }

                var continuation = new Continuation(SCOPE, () -> {
                    var buffer = ByteBuffer.allocateDirect(8192);
                    var io = new IO() {
                        @Override
                        public int read(ByteBuffer buffer) throws IOException {
                            int read;
                            while ((read = channel.read(buffer)) == 0) {
//                                println("read " + read);
                                key.interestOps(SelectionKey.OP_READ);
                                Continuation.yield(SCOPE);
                            }
                            key.interestOps(0);
                            return read;
                        }

                        @Override
                        public int write(ByteBuffer buffer) throws IOException {
                            int written;
                            while ((written = channel.write(buffer)) == 0) {
//                                println("written " + written);
                                key.interestOps(SelectionKey.OP_WRITE);
                                Continuation.yield(SCOPE);
                            }
                            key.interestOps(0);
                            return written;
                        }
                    };
                    try {
                        consumer.accept(SCOPE, buffer, io);
                    } catch (IOException | RuntimeException e) {
                        e.printStackTrace();
                    } finally {
                        key.cancel();
                        closeUnconditionally(channel);
                    }
                });
                key.attach(continuation);
                continuation.run();
                Continuation.yield(SCOPE);
            }
        });
        server.register(selector, SelectionKey.OP_ACCEPT, acceptContinuation);
        for (; ; ) {
            selector.select(key -> ((Continuation) key.attachment()).run());
        }
    }

    private static void fiberLoop(IOConsumer consumer, int localPort) throws IOException {
        System.out.println("start server on local port 7000");
        var server = ServerSocketChannel.open();
        server.configureBlocking(false);
        server.bind(new InetSocketAddress(localPort));
        var selector = Selector.open();
        server.register(selector, SelectionKey.OP_ACCEPT);
        for (; ; ) {
            selector.select(key -> FiberScope.background().schedule(() -> {
                try(SocketChannel channel = server.accept()) {
//                    channel.configureBlocking(false);
                    var buffer = ByteBuffer.allocateDirect(8192);
                    var io = new IO() {
                        @Override
                        public int read(ByteBuffer buffer) throws IOException {
                            return channel.read(buffer);
                        }

                        @Override
                        public int write(ByteBuffer buffer) throws IOException {
                            return channel.write(buffer);
                        }
                    };
                    consumer.accept(SCOPE, buffer, io);
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                    closeUnconditionally(server);
                    closeUnconditionally(selector);
                    return;
                }
            }));
        }
    }

    private static void threadLoop(IOConsumer consumer, int localPort) throws IOException {
        System.out.println("start server on local port 7000");
        var server = ServerSocketChannel.open();
        server.configureBlocking(true);
        server.bind(new InetSocketAddress(localPort));
        for (; ; ) {
            SocketChannel channel = server.accept();
            scheduler.submit(() -> {
                try(channel) {
                    var buffer = ByteBuffer.allocateDirect(8192);
                    var io = new IO() {
                        @Override
                        public int read(ByteBuffer buffer) throws IOException {
                            return channel.read(buffer);
                        }

                        @Override
                        public int write(ByteBuffer buffer) throws IOException {
                            return channel.write(buffer);
                        }
                    };
                    consumer.accept(SCOPE, buffer, io);
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                    closeUnconditionally(server);
                    return;
                }
            });
        }
    }

    public static void main(String[] args) throws IOException {
        AtomicInteger reqs = new AtomicInteger();
        IOConsumer consumer = (scope, buffer, io) -> {
            String line = io.readLine(buffer);
//            System.out.println("request " + line);
            if (line == null) {
                return;
            }

            reqs.incrementAndGet();

            String[] header = line.split(" ");
            String method = header[0];
            String path = header[1].substring(1);
            switch (method) {
                case "GET":
                    switch(path) {
                        case "token?value=nothing":
                            replyWithUnavailableToken(scope, io, 1);
                            break;
                        case "token?value=1":
                            replyWithUnavailableToken(scope, io, 2);
                            break;
                        case "token?value=2":
                            replyWithUnavailableToken(scope, io, 3);
                            break;
                        case "token?value=3":
                            replyWithUnavailableToken(scope, io, 4);
                            break;
                        case "token?value=4":
                        case "heartbeat?token=5":
                            replyWithAvailableToken(scope, io, 5);
                            break;
                        case "download":
                            io.write("HTTP/1.0 200 OK\nContent-Length:" + MAX_SIZE +"\n\n");
                            try(InputStream file = new RandomGeneratedInputStream(MAX_SIZE)) {
                                byte[] buff = new byte[8192];
                                while(file.read(buff) != -1) {
                                    ByteBuffer r = ByteBuffer.wrap(buff);
                                    do {
                                        io.write(r);
                                    } while(r.hasRemaining());
                                }
                            }
                            break;
                    }
                    break;
            }
        };

//        continuationLoop(consumer, 7000);
        fiberLoop(consumer, 7000);
//        threadLoop(consumer, 7000);
    }

    private static void replyWithUnavailableToken(ContinuationScope scope, IO io, int token) throws IOException {
        replyWithDelay(scope, io,
            String.format("Unavailable(eta=%s,wait=%s,token=%s)",
                random.nextInt((int) MAX_ETA_MS),
                random.nextInt(5_000),
                token));
    }

    private static void replyWithAvailableToken(ContinuationScope scope, IO io, int token) throws IOException {
        replyWithDelay(scope, io, String.format("Available(token=%s)", token));
    }

    private static void replyWithDelay(ContinuationScope scope, IO io, String value) throws IOException {
//        var k = Continuation.getCurrentContinuation(scope);
//        var delay = random.nextInt(5_000);
//        executor.schedule(k::run, delay, TimeUnit.MILLISECONDS);
//        Continuation.yield(scope);
//        try {
//            Thread.sleep(delay);
//        } catch (InterruptedException e) { e.printStackTrace(); }

        io.write("HTTP/1.0 200 Content-Length:" + value.length() +"\r\n\r\n");
        io.write(value);
    }
}
