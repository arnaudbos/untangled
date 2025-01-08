package io.monkeypatch.untangled;

import io.monkeypatch.untangled.utils.RandomGeneratedInputStream;

import jdk.internal.vm.Continuation;
import jdk.internal.vm.ContinuationScope;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.monkeypatch.untangled.FiberDemoServer.*;
import static io.monkeypatch.untangled.utils.IO.DEMO_SERVER_PORT;
import static io.monkeypatch.untangled.utils.IO.MAX_ETA_MS;
import static io.monkeypatch.untangled.utils.Log.err;
import static io.monkeypatch.untangled.utils.Log.println;

/**
 * - Continuations-based TCP server stolen from Rémi Forax (https://github.com/forax/loom-fiber/blob/master/src/main/java/fr.umlv.loom/fr/umlv/loom/Server.java)
 *   just tweaked a bit for routing and delaying responses via the scheduled executor
 * - fiber version and thread-based version for testing purposes
 * - TODO: Keep-Alive...
 */
public class FiberDemoServer {

    private static final Random random = new Random();
    private static final int MAX_SIZE = 10 * 1024 * 1024;

    static final ContinuationScope SCOPE = new ContinuationScope("server");
    static final ScheduledExecutorService continuationLoopScheduler = Executors.newSingleThreadScheduledExecutor();
    static final ExecutorService threadLoopPool = Executors.newCachedThreadPool();

    enum ServerMode {
        CONTINUATIONS,
        THREADS,
        VIRTUAL_THREADS,
    }

    public interface IO {
        int read(ByteBuffer buffer) throws IOException;
        int write(ByteBuffer buffer) throws IOException;
        void done() throws IOException;

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
        void accept(ContinuationScope scope, SocketChannel channel, ByteBuffer buffer, IO io) throws IOException;
    }

    static void closeUnconditionally(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            // do nothing
        }
    }

    static AtomicReference<ServerMode> mode = new AtomicReference<>();

    public static void main(String[] args) throws IOException {

        AtomicInteger reqs = new AtomicInteger();
        IOConsumer consumer = (scope, channel, buffer, io) -> {
            try(channel) {
                String line = io.readLine(buffer);
//            println("request " + line);
                if (line == null) {
                    return;
                }

                reqs.incrementAndGet();

                String[] header = line.split(" ");
                String method = header[0];
                String path = header[1].substring(1);
                switch (method) {
                    case "GET":
                        switch (path) {
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
                                io.write("HTTP/1.0 200 OK\r\nContent-Length:" + MAX_SIZE + "\r\n\r\n");
                                try (InputStream file = new RandomGeneratedInputStream(MAX_SIZE)) {
                                    byte[] buff = new byte[8192];
                                    for (int i = 0; file.read(buff) != -1; i++) {
                                        ByteBuffer r = ByteBuffer.wrap(buff);
//                                        println("" + i);
                                        if (i % 10 == 0) {
                                            var delay = random.nextInt(500);

                                            switch (mode.get()) {
                                                case CONTINUATIONS:
                                                    var k = Continuation.getCurrentContinuation(scope);
                                                    continuationLoopScheduler.schedule(k::run, delay, TimeUnit.MILLISECONDS);
                                                    Continuation.yield(scope);
                                                    break;
                                                case THREADS:
                                                case VIRTUAL_THREADS:
                                                    try {
                                                        Thread.sleep(delay);
                                                    } catch (InterruptedException e) { e.printStackTrace(); }
                                            }
                                        }
                                        do {
                                            io.write(r);
                                        } while (r.hasRemaining());
                                    }
                                }
                                break;
                        }
                        break;
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        };

        switch (ServerMode.THREADS) { // Play here
            case CONTINUATIONS:
                mode.set(ServerMode.CONTINUATIONS);
                ContinuationsLoop.start(consumer, DEMO_SERVER_PORT);
                break;
            case THREADS:
                mode.set(ServerMode.THREADS);
                ThreadsLoop.start(consumer, DEMO_SERVER_PORT);
                break;
            case VIRTUAL_THREADS:
                mode.set(ServerMode.VIRTUAL_THREADS);
                VirtualThreadsLoop.start(consumer, DEMO_SERVER_PORT);
                break;
        }
    }

    private static void replyWithUnavailableToken(ContinuationScope scope, IO io, int token) throws IOException {
        replyWithDelay(scope, io,
            String.format("Unavailable(eta=%s,wait=%s,token=%s)",
                random.nextInt((int) MAX_ETA_MS),
                random.nextInt(2_000),
                token));
    }

    private static void replyWithAvailableToken(ContinuationScope scope, IO io, int token) throws IOException {
        replyWithDelay(scope, io, String.format("Available(token=%s)", token));
    }

    private static void replyWithDelay(ContinuationScope scope, IO io, String value) throws IOException {
        var delay = random.nextInt(2_000);
        switch (mode.get()) {
            case CONTINUATIONS:
                var k = Continuation.getCurrentContinuation(scope);
                continuationLoopScheduler.schedule(k::run, delay, TimeUnit.MILLISECONDS);
                Continuation.yield(scope);
                break;
            case THREADS:
            case VIRTUAL_THREADS:
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) { e.printStackTrace(); }
                break;
        }

//        println("send header Content-Length:" + value.length());
//        io.write("HTTP/1.0 200 OK\n");
//        io.write("Content-Length:" + value.length() + "\n\n");
//        io.write("HTTP/1.0 200 Content-Length:" + value.length() +"\r\n\r\n");
//        println("send value: " + value);
//        io.write(value);
        var data = (value + "\r\n").getBytes();
        io.write("HTTP/1.1 200 OK\r\n");
        println("data length: " + data.length);
        io.write("Content-Length:" + data.length + "\r\n");
        io.write("Connection: close\r\n\r\n");
        io.write(ByteBuffer.wrap(data));
        io.done();
    }
}

class ContinuationsLoop {
    static void start(FiberDemoServer.IOConsumer consumer, int localPort) throws IOException {
        println("start server on local port " + localPort);
        var server = ServerSocketChannel.open();
        server.configureBlocking(false);
        server.bind(new InetSocketAddress(localPort));
        var selector = Selector.open();
        var acceptContinuation = new Continuation(SCOPE, () -> {
            for (; ; ) {
                SocketChannel channel;
                try {
//                    println("will accept");
                    channel = server.accept();
//                    println("did accept");
                } catch (IOException e) {
//                    err("failed");
                    e.printStackTrace();
                    closeUnconditionally(server);
                    closeUnconditionally(selector);
                    return;
                }
                SelectionKey key;
                try {
                    channel.configureBlocking(false);
//                    println("register selector");
                    key = channel.register(selector, 0);
                } catch (IOException e) {
//                    err("failed to register selector");
                    e.printStackTrace();
                    closeUnconditionally(channel);
                    return;
                }

                var continuation = new Continuation(SCOPE, () -> {
//                    println("continuation start");
                    var buffer = ByteBuffer.allocateDirect(8192);
                    var io = new FiberDemoServer.IO() {
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
                            println("writing " + buffer.toString());
                            while ((written = channel.write(buffer)) == 0) {
                                println("written " + written);
                                key.interestOps(SelectionKey.OP_WRITE);
                                println("yield continuation");
                                Continuation.yield(SCOPE);
                                println("restart continuation");
                            }
                            key.interestOps(0);
                            return written;
                        }

                        @Override
                        public void done() throws IOException {
                            channel.shutdownOutput();
                            channel.close();
                        }
                    };
                    try {
//                        println("io consumer will run");
                        consumer.accept(SCOPE, channel, buffer, io);
//                        println("io consumer did run");
                    } catch (Exception e) {
//                        err("io consumer did fail");
                        e.printStackTrace();
                    } finally {
                        key.cancel();
                        closeUnconditionally(channel);
                    }
                });
                key.attach(continuation);
//                println("key attached");
                continuation.run();
                Continuation.yield(SCOPE);
            }
        });
        server.register(selector, SelectionKey.OP_ACCEPT, acceptContinuation);
        for (; ; ) {
            selector.select(key -> ((Continuation) key.attachment()).run());
        }
    }
}

class VirtualThreadsLoop {
    static void start(FiberDemoServer.IOConsumer consumer, int localPort) throws IOException {
        println("start server on local port " + localPort);
        var server = ServerSocketChannel.open();
        server.configureBlocking(false);
        server.bind(new InetSocketAddress(localPort));
        var selector = Selector.open();
        server.register(selector, SelectionKey.OP_ACCEPT);
        var i = new AtomicInteger(0);
        for (; ; ) {
            selector.select(key -> Thread.ofVirtual().name("acceptor-" + i.get()).start(() -> {
                try(SocketChannel channel = server.accept()) {
//                    channel.configureBlocking(false);
                    var buffer = ByteBuffer.allocateDirect(8192);
                    var io = new FiberDemoServer.IO() {
                        @Override
                        public int read(ByteBuffer buffer) throws IOException {
                            return channel.read(buffer);
                        }

                        @Override
                        public int write(ByteBuffer buffer) throws IOException {
                            return channel.write(buffer);
                        }

                        @Override
                        public void done() throws IOException {
                            channel.shutdownOutput();
                            channel.close();
                        }
                    };
                    consumer.accept(SCOPE, channel, buffer, io);
                } catch (IOException e) {
                    err(e.getMessage());
                    closeUnconditionally(server);
                    closeUnconditionally(selector);
                }
            }));
        }
    }
}

class ThreadsLoop {
    static void start(FiberDemoServer.IOConsumer consumer, int localPort) throws IOException {
        println("start server on local port " + localPort);
        var server = ServerSocketChannel.open();
        server.configureBlocking(true);
        server.bind(new InetSocketAddress(localPort));
        for (; ; ) {
            SocketChannel channel = server.accept();
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            channel.setOption(StandardSocketOptions.SO_SNDBUF, 65536);
            channel.setOption(StandardSocketOptions.SO_RCVBUF, 65536);
            threadLoopPool.submit(() -> {
                try(channel) {
                    var buffer = ByteBuffer.allocateDirect(8192);
                    var io = new FiberDemoServer.IO() {
                        @Override
                        public int read(ByteBuffer buffer) throws IOException {
                            return channel.read(buffer);
                        }

                        @Override
                        public int write(ByteBuffer buffer) throws IOException {
                            return channel.write(buffer);
                        }

                        @Override
                        public void done() throws IOException {
                            channel.shutdownOutput();
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            channel.close();
                        }
                    };
                    consumer.accept(SCOPE, channel, buffer, io);
                } catch (IOException e) {
                    err(e.getMessage());
                    closeUnconditionally(server);
                }
            });
        }
    }
}