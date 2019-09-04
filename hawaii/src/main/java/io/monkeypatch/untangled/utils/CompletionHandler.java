package io.monkeypatch.untangled.utils;

public interface CompletionHandler<V> {

    void completed(V result);

    void failed(Throwable t);
}
