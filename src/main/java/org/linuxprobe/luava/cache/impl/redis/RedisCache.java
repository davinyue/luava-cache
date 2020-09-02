package org.linuxprobe.luava.cache.impl.redis;

import org.linuxprobe.luava.cache.Cache;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionCommands;
import org.springframework.data.redis.connection.RedisServerCommands;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class RedisCache implements Cache {
    public static final String RedisKeyCharset = "UTF-8";

    private final RedisTemplate<Serializable, Serializable> redisTemplate;

    public RedisTemplate<Serializable, Serializable> getRedisTemplate() {
        return this.redisTemplate;
    }

    public RedisCache(RedisTemplate<Serializable, Serializable> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * @param key     key
     * @param value   value
     * @param timeout timeout
     */
    @Override
    public <V extends Serializable, K extends Serializable> void set(K key, V value, long timeout) {
        if (timeout == 0) {
            this.redisTemplate.opsForValue().set(key, value);
        } else {
            this.redisTemplate.opsForValue().set(key, value, timeout, TimeUnit.SECONDS);
        }
    }

    @Override
    public <V extends Serializable, K extends Serializable> void set(K key, V value, long timeout, TimeUnit timeUnit) {
        if (timeout == 0) {
            this.redisTemplate.opsForValue().set(key, value);
        } else {
            this.redisTemplate.opsForValue().set(key, value, timeout, timeUnit);
        }
    }

    /**
     * @param key   key
     * @param value value
     */
    @Override
    public <V extends Serializable, K extends Serializable> void set(K key, V value) {
        this.redisTemplate.opsForValue().set(key, value);
    }

    /**
     * @param key 键
     */
    @Override
    @SuppressWarnings("unchecked")
    public <V extends Serializable, K extends Serializable> V get(K key) {
        return (V) this.redisTemplate.opsForValue().get(key);
    }

    /**
     * 获取后设置到期时间
     *
     * @param key     键
     * @param timeout 有效期,单位秒
     */
    @SuppressWarnings("unchecked")
    public <T extends Serializable> T getAndSetExpire(final String key, final long timeout) {
        T result = (T) this.redisTemplate.opsForValue().get(key);
        if (result != null) {
            this.setExpire(key, timeout);
        }
        return result;
    }

    /**
     * 是否存在key
     *
     * @param key 键
     */
    public boolean exists(final String key) {
        Boolean result = this.redisTemplate.hasKey(key);
        return result == null ? false : result;
    }

    public Boolean setNx(final String key, final String value) {
        return this.redisTemplate.opsForValue().setIfAbsent(key, value);
    }

    /**
     * 设置key的到期时间
     *
     * @param key  键
     * @param date 到期时间
     * @return 设置成功，返回true
     */
    public Boolean setExpireAt(final String key, final Date date) {
        return this.redisTemplate.expireAt(key, date);
    }

    /**
     * 设置过期时间，单位：秒
     *
     * @param key     key值
     * @param timeout 过期秒数
     */
    @Override
    public <T extends Serializable> void setExpire(T key, long timeout) {
        this.redisTemplate.expire(key, timeout, TimeUnit.SECONDS);
    }

    @Override
    public void flushDB() {
        this.redisTemplate.execute((RedisCallback<Void>) connection -> {
            connection.flushDb();
            return null;
        });
    }

    @Override
    public long dbSize() {
        Long result = this.redisTemplate.execute(RedisServerCommands::dbSize);
        return result == null ? 0 : result;
    }

    public String ping() {
        return this.redisTemplate.execute(RedisConnectionCommands::ping);
    }

    /**
     * delete
     *
     * @param keys keys
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T extends Serializable> void delete(T... keys) {
        if (keys == null || keys.length == 0) {
            return;
        }
        this.redisTemplate.execute((RedisCallback<Void>) connection -> {
            RedisSerializer<Object> serializer = (RedisSerializer<Object>) RedisCache.this.redisTemplate
                    .getKeySerializer();
            for (T key : keys) {
                connection.del(serializer.serialize(key));
            }
            return null;
        });
    }

    /**
     * delete
     *
     * @param keys keys
     */
    @Override
    public <T extends Serializable> void delete(final Collection<T> keys) {
        if (keys == null || keys.size() == 0) {
            return;
        }
        this.redisTemplate.execute((RedisCallback<Void>) connection -> {
            @SuppressWarnings("unchecked")
            RedisSerializer<Object> serializer = (RedisSerializer<Object>) RedisCache.this.redisTemplate
                    .getKeySerializer();
            for (Serializable key : keys) {
                connection.del(serializer.serialize(key));
            }
            return null;
        });
    }

    /**
     * @param pattern 正则表达式
     */
    @Override
    @SuppressWarnings("unchecked")
    public <V extends Serializable> Set<V> keys(final String pattern) {
        return (Set<V>) this.redisTemplate.keys(pattern);
    }

    @Override
    public <V extends Serializable, K extends Serializable> boolean setNX(K key, V value) {
        Boolean result = this.redisTemplate.opsForValue().setIfAbsent(key, value);
        if (result == null) {
            result = false;
        }
        return result;
    }

    @Override
    public <V extends Serializable, K extends Serializable> Serializable getAndSet(K key, V value) {
        return this.redisTemplate.opsForValue().getAndSet(key, value);
    }

    /**
     * @param pattern 正则表达式
     */
    public Set<String> scan(String pattern) {
        Set<String> keys = new HashSet<>();
        this.scan(pattern, item -> {
            //符合条件的key
            String key = new String(item, StandardCharsets.UTF_8);
            keys.add(key);
        });
        return keys;
    }

    /**
     * scan
     *
     * @param pattern  表达式
     * @param consumer 对迭代到的key进行操作
     */
    public void scan(String pattern, Consumer<byte[]> consumer) {
        this.redisTemplate.execute((RedisConnection connection) -> {
            Cursor<byte[]> cursor = connection.scan(ScanOptions.scanOptions().count(Long.MAX_VALUE).match(pattern)
                    .build());
            cursor.forEachRemaining(consumer);
            return null;
        });
    }
}
