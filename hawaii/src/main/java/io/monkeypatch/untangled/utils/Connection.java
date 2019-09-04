package io.monkeypatch.untangled.utils;

public class Connection {
    private final String token;

    private Connection(String token) {
        this.token = token;
    }

    public String getToken() {
        return token;
    }

    public static final class Available extends Connection {
        public Available(String token) {
            super(token);
        }
    }

    public static final class Unavailable extends Connection {
        private final long eta;
        private final long wait;

        public Unavailable(long eta, long wait, String token) {
            super(token);
            this.eta = eta;
            this.wait = wait;
        }

        public long getEta() {
            return eta;
        }

        public long getWait() {
            return wait;
        }
    }
}
