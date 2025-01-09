package io.monkeypatch.untangled.utils;

@FunctionalInterface
public interface CheckedSupplier<T> {
    T get() throws Exception;
}