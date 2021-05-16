package io.github.vipcxj.redis.lock;

import java.util.Objects;

public class CtxLockKey {
    private final String value;

    public CtxLockKey(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CtxLockKey that = (CtxLockKey) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
