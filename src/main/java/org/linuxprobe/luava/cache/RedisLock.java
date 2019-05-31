package org.linuxprobe.luava.cache;

import java.util.Date;
import java.util.Random;

import org.linuxprobe.luava.cache.impl.RedisCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * redis锁实现
 * 
 * @author larry
 */
public class RedisLock {
	private static final Logger logger = LoggerFactory.getLogger(RedisLock.class);
	private static ThreadLocal<String> threadLocalValue = new ThreadLocal<>();

	/**
	 * 获取锁，该方法是线程安全的
	 * 
	 * @param redisCache redisCache
	 * @param lockKey    锁标记
	 * @param expires    锁过有效期,单位秒,防止服务挂掉后锁不能释放产生死锁
	 * @return 获取锁成功返回true，失败返回false
	 */
	public static boolean lock(RedisCache redisCache, String lockKey, long expires) {
		String value = String.valueOf(((new Date().getTime()) / 1000) + expires);
		boolean setSuccess = redisCache.setNX(lockKey, value);
		boolean result = false;
		/** 如果不成功 */
		if (!setSuccess) {
			String currentValue = redisCache.get(lockKey);
			/** 如果取出的到期时间戳已经小于当前时间戳，则说明加锁的系统挂掉了 */
			if (currentValue != null && Long.parseLong(currentValue) < (new Date().getTime() / 1000)) {
				String newValue = String.valueOf(((new Date().getTime()) / 1000) + expires);
				String oldValue = (String) redisCache.getAndSet(lockKey, newValue);
				/** 如果设置成功后返回的值等于设置前获取的值，说明还未被其它线程或服务抢占，可以获得锁 */
				if (oldValue != null && oldValue.equals(currentValue)) {
					result = true;
					threadLocalValue.set(newValue);
					logger.debug(lockKey + "存在无效锁，重置并加锁成功");
				}
			}
		} else {
			result = true;
			threadLocalValue.set(value);
			logger.debug(lockKey + "加锁成功");
		}
		if (!result) {
			logger.debug(lockKey + "加锁失败");
		}
		return result;
	}

	/** 释放锁，该方法是线程安全的，线程睡眠0-100毫秒，可以防止饥饿线程的出现 */
	public static void unlock(RedisCache redisCache, String lockKey) {
		String value = redisCache.get(lockKey);
		if (value != null) {
			if (value.equals(threadLocalValue.get())) {
				redisCache.delete(lockKey);
				logger.debug(lockKey + "解锁成功");
			} else {
				logger.debug(lockKey + "锁已经其它线程或服务器重置，无需解锁");
			}
		} else {
			logger.debug(lockKey + "无锁，无需解锁");
		}
		threadLocalValue.remove();
		Random random = new Random();
		try {
			Thread.sleep(random.nextInt(100));
		} catch (InterruptedException e) {
			new RuntimeException(e);
		}
	}
}