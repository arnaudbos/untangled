package io.monkeypatch.untangled.experiments;

public class Experiment01_SimpleContinuation {
    public static void main(String[] args) {
        ContinuationScope scopeA = new ContinuationScope("scopeA");
        Continuation c = new Continuation(scopeA, () -> {
            System.out.println("1");
            Continuation.yield(scopeA);

            System.out.println("2");
            Continuation.yield(scopeA);

            System.out.println("3");
            Continuation.yield(scopeA);

            System.out.println("4");
        });
        while(!c.isDone()) {
            System.out.println("running...");
            c.run();
        }
        System.out.println("done");
    }
}
