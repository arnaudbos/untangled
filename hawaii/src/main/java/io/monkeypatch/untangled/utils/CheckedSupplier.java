package io.monkeypatch.untangled.utils;

import java.io.IOException;

@FunctionalInterface
public interface CheckedSupplier<T> {
    T get() throws IOException;
}