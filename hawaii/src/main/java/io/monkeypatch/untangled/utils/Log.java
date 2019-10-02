package io.monkeypatch.untangled.utils;

public class Log {
    public static void println(String msg){
        System.out.println("INFO [" + Thread.currentThread().getName() + "] " + msg);
    }

    public static void err(String msg){
        System.out.println("ERRRO [" + Thread.currentThread().getName() + "] " + msg);
    }
}
