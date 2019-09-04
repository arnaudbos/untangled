package io.monkeypatch.untangled.experiments;

public class Log {
    public static void println(String msg){
        System.out.println("INFO [" + Thread.currentThread().getName() + "] " + msg);
    }

    public static void err(String msg){
        System.out.println("ERRO [" + Thread.currentThread().getName() + "] " + msg);
    }
}
