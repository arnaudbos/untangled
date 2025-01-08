package io.monkeypatch.untangled.experiments;

import jdk.internal.vm.Continuation;
import jdk.internal.vm.ContinuationScope;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Experiment03_TryWithResource {
    public static void main(String[] args) {
        ContinuationScope scopeA = new ContinuationScope("scopeA");
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        System.out.println("init");

        InputStream is = new RandomGeneratedInputStream(10);
        Continuation c = new Continuation(scopeA, () -> {
            try(is) {
                System.out.println("start");

                Continuation k = Continuation.getCurrentContinuation(scopeA);
                scheduler.schedule(() -> {
                    System.out.println("restarting k");
                    k.run();
                }, 2, TimeUnit.SECONDS);
                Continuation.yield(scopeA);

                System.out.println("stop");
                scheduler.shutdownNow();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        c.run();

        System.out.println("done");
    }
}
