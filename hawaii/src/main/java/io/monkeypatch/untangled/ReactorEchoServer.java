package io.monkeypatch.untangled;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;

import static io.monkeypatch.untangled.utils.IO.DEMO_SERVER_PORT;
import static io.monkeypatch.untangled.utils.Log.println;

public class ReactorEchoServer {

    public static void main(String[] args) {

        HttpServer httpServer = HttpServer.create()
            .host("localhost")
            .port(DEMO_SERVER_PORT)
            .handle((request, response) -> response.send(request.receive().retain()));

        httpServer.warmup()
            .block();

        httpServer.bindNow()
            .onDispose()
            .block();
    }

    public static class ReactorEchoClient {

        public static void main(String[] args) {

            HttpClient client =
                HttpClient.create()
                    .host("localhost")
                    .port(7001);

            client.warmup()
                .block();

            Flux.range(0, 2000)
                .flatMap(_ -> client.post()
                    .uri("/echo")
                    .send(ByteBufFlux.fromString(Mono.just("hello")))
                    .responseContent()
                    .aggregate()
                    .asString()
                )
                .doOnNext(s -> println("Response:: " + s))
                .blockLast();
        }
    }
}