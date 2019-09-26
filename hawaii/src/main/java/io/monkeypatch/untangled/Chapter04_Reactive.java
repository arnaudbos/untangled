package io.monkeypatch.untangled;

import io.monkeypatch.untangled.utils.Connection;
import io.monkeypatch.untangled.utils.EtaExceededException;
import io.monkeypatch.untangled.utils.IO;
import io.monkeypatch.untangled.utils.Log;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.retry.Repeat;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.monkeypatch.untangled.utils.IO.*;
import static io.monkeypatch.untangled.utils.Log.println;

public class Chapter04_Reactive {

    private static final int MAX_CLIENTS = 50;

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
    private Mono<String> getThingy(int i) {
        return Mono.defer(() -> {
            println(i + ":: Start getThingy.");
            return getConnection();
        }).flatMap(conn -> {
            AtomicInteger total = new AtomicInteger();
            return gateway.downloadThingy(conn.getToken())
                .takeUntilOther(makePulse(conn))
                .doOnNext(bytes -> println("read " + bytes.length + " bytes."))
                .doOnNext(b -> total.updateAndGet(t -> t+b.length))
//                .takeWhile(b -> total.get()<MAX_SIZE)
                .then(Mono.just(i + ":: Download finished"));
        });
    }
    //</editor-fold>

    //<editor-fold desc="Pulse: actually that's nice">
    private Mono<Void> makePulse(Connection.Available conn) {
        return Flux.interval(Duration.ofSeconds(2L))
            .flatMap(l -> coordinator.heartbeat(conn.getToken()))
            .doOnNext(c -> println("Pulse!"))
            .then()
            .doOnTerminate(() -> println("Pulse terminated"));
    }
    //</editor-fold>

    //<editor-fold desc="Run: simulate client calls">
    private void run() {
        Flux.range(0, MAX_CLIENTS)
            .flatMap(this::getThingy)
            .delaySubscription(Duration.ofSeconds(5L))
            .blockLast();
    }

    public static void main(String[] args) {
        IO.init_Chapter04_Reactive();
        (new Chapter04_Reactive()).run();
        println("Done.");
    }
    //</editor-fold>
}


class ReactiveCoordinatorService {
    private static final Random random = new Random();

    Mono<Connection> requestConnection(String token) {
        return Mono.just("requestConnection(String token)")
            .doOnNext(Log::println)
            .flatMapMany(o -> reactiveRequest("http://localhost:7000/token?value=" + (token == null ? "nothing" : token)))
            .publishOn(Schedulers.parallel())
            .then(Mono.fromSupplier(() -> {
                int attempt = token == null ? 0 : Integer.parseInt(token);

                return attempt > 4
                    ? new Connection.Available("Ahoy!")
                    : new Connection.Unavailable(20_000L, random.nextInt(2_000), String.valueOf(attempt + 1));
            }));
    }

    Mono<Connection> heartbeat(String token) {
        return Mono.just("heartbeat(String token)")
            .doOnNext(Log::println)
            .flatMapMany(o -> reactiveRequest("http://localhost:7000/heartbeat?token=" + token))
            .publishOn(Schedulers.parallel())
            .then(Mono.just(new Connection.Available("Ahoy!")));
    }
}

class ReactiveGatewayService {
    Flux<byte[]> downloadThingy(String token) {
        return reactiveRequest("http://localhost:7000/download")
            .publishOn(Schedulers.parallel())
            ;
    }
}