package io.monkeypatch.untangled.utils;

public interface RequestHandler {

    boolean isCancelled();

    void received(byte[] data);

    void completed();

    void failed(Throwable t);
}
