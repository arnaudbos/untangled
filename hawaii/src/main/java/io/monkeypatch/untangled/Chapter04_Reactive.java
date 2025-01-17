package io.monkeypatch.untangled;

import io.monkeypatch.untangled.utils.Connection;
import io.monkeypatch.untangled.utils.EtaExceededException;
import io.monkeypatch.untangled.utils.IO;
import io.monkeypatch.untangled.utils.Log;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.retry.Repeat;

import javax.net.ssl.SSLException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collector;

import static io.monkeypatch.untangled.utils.IO.DEMO_SERVER_URL;
import static io.monkeypatch.untangled.utils.IO.MAX_ETA_MS;
import static io.monkeypatch.untangled.utils.IO.MAX_SIZE;
import static io.monkeypatch.untangled.utils.IO.reactiveHttpRequest;
import static io.monkeypatch.untangled.utils.Log.err;
import static io.monkeypatch.untangled.utils.Log.println;
import static reactor.core.scheduler.Schedulers.DEFAULT_POOL_SIZE;
import static reactor.core.scheduler.Schedulers.newParallel;

public class Chapter04_Reactive {

    private static final int MAX_CLIENTS = 200;

    private final ReactiveCoordinatorService coordinator = new ReactiveCoordinatorService();
    private final ReactiveGatewayService gateway = new ReactiveGatewayService();

    //<editor-fold desc="Token calls: Atomics, defer, delay and repeatWhenEmpty festival">
    private Mono<Connection.Available> getConnection() {
        return getConnection(0, 0, null);
    }

    private Mono<Connection.Available> getConnection(long eta, long wait, String token) {
        AtomicLong etaRef = new AtomicLong(eta);
        AtomicLong waitRef = new AtomicLong(wait);
        AtomicReference<String> tokenRef = new AtomicReference<>(token);
        return Mono.defer(() -> {
            if (etaRef.get() > MAX_ETA_MS) {
                return Mono.error(new EtaExceededException());
            }
            return Mono.delay(Duration.ofMillis(waitRef.get()))
                .flatMap(i -> coordinator.requestConnection(tokenRef.get()));
        }).flatMap(c -> {
            if (c instanceof Connection.Available) {
                return Mono.just((Connection.Available) c);
            } else {
                Connection.Unavailable unavail = (Connection.Unavailable) c;
                etaRef.set(unavail.getEta());
                waitRef.set(unavail.getWait());
                tokenRef.set(unavail.getToken());
                return Mono.empty();
            }
        }).repeatWhenEmpty(Repeat
            .onlyIf(ctx -> true)
            .doOnRepeat(ctx ->
                println(waitRef.get() + ", " + etaRef.get() + ", " + tokenRef.get())));
    }
    //</editor-fold>

    //<editor-fold desc="Main 'controller' function (getThingy): defer is mysterious, AtomicInteger is weird and takeUntilOther feels like a hack">
    private Mono<Integer> getThingy(int i) {
        return Mono.defer(() -> {
            println(i + ":: Start getThingy.");
            return getConnection();
        }).flatMap(conn -> {
            AtomicInteger total = new AtomicInteger();
            return gateway.downloadThingy(conn.getToken())
                .takeUntilOther(makePulse(conn))
//                .doOnNext(bytes -> println("read " + bytes.length + " bytes."))
                .doOnNext(b -> total.updateAndGet(t -> t+b.length))
                .takeWhile(b -> total.get()<MAX_SIZE)
                .then(Mono.just(i));
        });
    }
    //</editor-fold>

    //<editor-fold desc="Pulse: actually that's nice">
    private Mono<Void> makePulse(Connection.Available conn) {
        return Flux.interval(Duration.ofSeconds(2L))
            .flatMap(l -> coordinator.heartbeat(conn.getToken()))
            .doOnNext(c -> println("Pulse!"))
            .then()
            .doOnCancel(() -> println("Pulse cancelled"))
            .doOnTerminate(() -> println("Pulse terminated"));
    }
    //</editor-fold>

    //<editor-fold desc="Run: simulate client calls">
    private void run() {
        Flux.range(0, MAX_CLIENTS)
            .subscribeOn(Schedulers.single())
            .flatMap(this::getThingy)
            .doOnNext(i -> println(i + ":: Download succeeded"))
            .doOnError(t -> {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                t.printStackTrace(pw);
                err("Download failed " + sw.toString());
            })
            .doOnTerminate(() -> {
                println("Done.");
            })
//            .delaySubscription(Duration.ofSeconds(15L))
            .blockLast();
    }

    public static void main(String[] args) throws SSLException {
        IO.init_Chapter04_Reactive_Http();
//        IO.init_Chapter04_Reactive_Tcp();
        (new Chapter04_Reactive()).run();
    }
    //</editor-fold>
}


class ReactiveCoordinatorService {
    private static final Scheduler REQUEST_PARALLEL = newParallel("request-parallel", DEFAULT_POOL_SIZE, true);
    private static final Scheduler PULSE_PARALLEL = newParallel("pulse-parallel", DEFAULT_POOL_SIZE, true);

    Mono<Connection> requestConnection(String token) {
        return parseToken(
            Mono.just("requestConnection(String token)")
                .subscribeOn(REQUEST_PARALLEL)
                .doOnNext(Log::println)
//                .flatMapMany(_ -> reactiveTcpRequest(DEMO_SERVER_URL,
//                    String.format(HEADERS_TEMPLATE, "GET", "token?value=" + (token == null ? "nothing" : token), "text/*", String.valueOf(0)))
//                )
                .flatMapMany(_ -> reactiveHttpRequest(DEMO_SERVER_URL + "/token?value=" + (token == null ? "nothing" : token)))
                .publishOn(REQUEST_PARALLEL)
        );
    }

    Mono<Connection> heartbeat(String token) {
        return parseToken(
            Mono.just("heartbeat(String token)")
                .subscribeOn(PULSE_PARALLEL)
                .doOnNext(Log::println)
//                .flatMapMany(o -> reactiveTcpRequest(DEMO_SERVER_URL,
//                    String.format(HEADERS_TEMPLATE, "GET",    "/heartbeat?token=" + token, "text/*", String.valueOf(0))
//                ))
                .flatMapMany(_ -> reactiveHttpRequest(DEMO_SERVER_URL + "/heartbeat?token=" + token))
                .publishOn(PULSE_PARALLEL)
        );
    }

    private Mono<Connection> parseToken(Flux<byte[]> f) {
        return f
            .map(String::new)
            .doOnNext(s -> println("token piece " + s))
            .collect(Collector.of(
                StringBuilder::new,
                StringBuilder::append,
                (left, right) -> left,
                StringBuilder::toString))
            .doOnNext(s -> println("token to parse " + s))
            .map(IO::parseConnection)
            .doOnError(t -> {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                t.printStackTrace(pw);
                err("token error " + sw.toString());
            })
            .doOnCancel(() -> err("token cancelled"))
            ;
    }
}

class ReactiveGatewayService {

    private static final Scheduler DOWNLOAD_PARALLEL = newParallel("download-parallel", DEFAULT_POOL_SIZE, true);

    Flux<byte[]> downloadThingy(String token) {
//        return reactiveTcpRequest(DEMO_SERVER_URL,
//            String.format(HEADERS_TEMPLATE, "GET", "download", "text/*", String.valueOf(0))
//        )
        return reactiveHttpRequest(DEMO_SERVER_URL + "/download")
            .publishOn(DOWNLOAD_PARALLEL)
            ;
    }
}
