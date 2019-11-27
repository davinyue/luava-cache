package org.linuxprobe.luava.cache.lock;

public interface Lock {
    public void lock(String lockKey, long expires);

    public void lockInterruptibly(String lockKey, long expires);

    public boolean tryLock(String lockKey, long expires);

    public boolean tryLock(String lockKey, long expires, long wait);

    public void unlock(String lockKey);
}
