package org.linuxprobe.luava.cache.lock;

import org.linuxprobe.luava.cache.impl.RedisCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Random;

/**
 * redis悲观锁
 */
public class ReidsReentrantLock implements Lock {
    private static final Logger logger = LoggerFactory.getLogger(ReidsReentrantLock.class);
    private static ThreadLocal<String> threadLocalValue = new ThreadLocal<>();
    private RedisCache redisCache;

    public ReidsReentrantLock(RedisCache redisCache) {
        this.redisCache = redisCache;
    }

    @Override
    public void lock(String lockKey, long expires) {
        while (!this.tryLock(lockKey, expires)) {
        }
    }

    @Override
    public void lockInterruptibly(String key, long expires) {

    }

    @Override
    public boolean tryLock(String lockKey, long expires) {
        String value = String.valueOf(((new Date().getTime()) / 1000) + expires);
        boolean setSuccess = this.redisCache.setNX(lockKey, value);
        boolean result = false;
        /** 如果不成功 */
        if (!setSuccess) {
            String currentValue = this.redisCache.get(lockKey);
            /** 如果取出的到期时间戳已经小于当前时间戳，则说明加锁的系统挂掉了 */
            if (currentValue != null && Long.parseLong(currentValue) < (new Date().getTime() / 1000)) {
                String newValue = String.valueOf(((new Date().getTime()) / 1000) + expires);
                String oldValue = (String) this.redisCache.getAndSet(lockKey, newValue);
                /** 如果设置成功后返回的值等于设置前获取的值，说明还未被其它线程或服务抢占，可以获得锁 */
                if (oldValue != null && oldValue.equals(currentValue)) {
                    result = true;
                    ReidsReentrantLock.threadLocalValue.set(newValue);
                    ReidsReentrantLock.logger.debug(lockKey + "存在无效锁，重置并加锁成功");
                }
            }
        } else {
            result = true;
            ReidsReentrantLock.threadLocalValue.set(value);
            ReidsReentrantLock.logger.debug(lockKey + "加锁成功");
        }
        if (!result) {
            ReidsReentrantLock.logger.debug(lockKey + "加锁失败");
        }
        return result;
    }

    @Override
    public boolean tryLock(String lockKey, long expires, long wait) {
        long currentTime = System.currentTimeMillis();
        boolean result = false;
        while (true) {
            if (System.currentTimeMillis() - currentTime > wait) {
                break;
            }
            result = this.tryLock(lockKey, expires);
            if (result) {
                break;
            }
        }
        return result;
    }

    @Override
    public void unlock(String lockKey) {
        String value = this.redisCache.get(lockKey);
        if (value != null) {
            if (value.equals(ReidsReentrantLock.threadLocalValue.get())) {
                this.redisCache.delete(lockKey);
                ReidsReentrantLock.logger.debug(lockKey + "解锁成功");
            } else {
                ReidsReentrantLock.logger.debug(lockKey + "锁已经其它线程或服务器重置，无需解锁");
            }
        } else {
            ReidsReentrantLock.logger.debug(lockKey + "无锁，无需解锁");
        }
        ReidsReentrantLock.threadLocalValue.remove();
        Random random = new Random();
        try {
            Thread.sleep(random.nextInt(100));
        } catch (InterruptedException e) {
            new RuntimeException(e);
        }
    }
}
