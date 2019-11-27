package org.linuxprobe.luava.cache.impl.redis;

import org.linuxprobe.luava.cache.Cache;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class RedisCache implements Cache {
    public static final String RedisKeyCharset = "UTF-8";

    private RedisTemplate<Serializable, Serializable> redisTemplate;

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
     * @param key
     */
    @Override
    @SuppressWarnings("unchecked")
    public <V extends Serializable, K extends Serializable> V get(K key) {
        return (V) this.redisTemplate.opsForValue().get(key);
    }

    /**
     * 获取后设置到期时间
     *
     * @param key
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
     * @param key key
     */
    public boolean exists(final String key) {
        return this.redisTemplate.hasKey(key);
    }

    public Boolean setNx(final String key, final String value) {
        return this.redisTemplate.opsForValue().setIfAbsent(key, value);
    }

    /**
     * @param key
     * @return value
     */
    public String getStringFromList(final String key) {
        return (String) this.redisTemplate.execute(new RedisCallback<Object>() {
            @Override
            public String doInRedis(final RedisConnection connection) throws DataAccessException {
                return new String(connection.rPop(key.getBytes(Charset.forName(RedisCache.RedisKeyCharset))),
                        Charset.forName(RedisCache.RedisKeyCharset));
            }
        });
    }

    public long getListLength(final String key) {
        return this.redisTemplate.execute(new RedisCallback<Long>() {
            @Override
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return connection.lLen(key.getBytes());
            }
        });
    }

    /**
     * 向set中存入一个或多个元素
     *
     * @param key
     * @param elements byte[] 数组，可同时添加多个元素
     */
    public Long addElementsToSet(final String key, final byte[]... elements) {
        return this.redisTemplate.execute(new RedisCallback<Long>() {
            @Override
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                if (connection.isClosed()) {
                    connection = RedisCache.this.redisTemplate.getConnectionFactory().getConnection();
                }
                Long effectLen = connection.sAdd(key.getBytes(), elements);
                return effectLen;
            }
        });
    }

    /**
     * 从set中删除元素
     *
     * @param key
     * @param elements byte[] 数组，可同时删除多个元素
     */
    public Long remElementsFromSet(final String key, final byte[]... elements) {
        return this.redisTemplate.execute(new RedisCallback<Long>() {
            @Override
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                if (connection.isClosed()) {
                    connection = RedisCache.this.redisTemplate.getConnectionFactory().getConnection();
                }
                Long effectLen = connection.sRem(key.getBytes(Charset.forName(RedisCache.RedisKeyCharset)), elements);
                return effectLen;
            }
        });
    }

    /**
     * 获取set中元素个数
     *
     * @param key key
     */
    public Long getSetSize(final String key) {
        return this.redisTemplate.execute(new RedisCallback<Long>() {
            @Override
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                if (connection.isClosed()) {
                    connection = RedisCache.this.redisTemplate.getConnectionFactory().getConnection();
                }
                Long effectLen = connection.sCard(key.getBytes(Charset.forName(RedisCache.RedisKeyCharset)));
                return effectLen;
            }
        });
    }

    /**
     * 从set中获取一个元素，并同时删除该元素
     *
     * @param key key
     * @return byte数组，请根据上下文转换成相应的类型
     */
    public byte[] getAndRemoveElementFromSet(final String key) {
        return this.redisTemplate.execute(new RedisCallback<byte[]>() {
            @Override
            public byte[] doInRedis(RedisConnection connection) throws DataAccessException {
                if (connection.isClosed()) {
                    connection = RedisCache.this.redisTemplate.getConnectionFactory().getConnection();
                }
                byte[] element = connection.sPop(key.getBytes(Charset.forName(RedisCache.RedisKeyCharset)));
                return element;
            }
        });
    }

    /**
     * 获取到一个set中所有元素
     *
     * @param key key
     * @return 该set中所有元素的byte[]，请根据上下文转换成相应的类型
     */
    public Set<byte[]> getElementsFromSet(final String key) {
        return this.redisTemplate.execute(new RedisCallback<Set<byte[]>>() {
            @Override
            public Set<byte[]> doInRedis(RedisConnection connection) throws DataAccessException {
                if (connection.isClosed()) {
                    connection = RedisCache.this.redisTemplate.getConnectionFactory().getConnection();
                }
                Set<byte[]> elements = connection.sMembers(key.getBytes(Charset.forName(RedisCache.RedisKeyCharset)));
                return elements;
            }
        });
    }

    /**
     * 设置key的到期时间
     *
     * @param key
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

    /**
     * 发布消息到一个指定频道
     *
     * @param channel 频道
     * @param message 消息体
     */
    public void publish(final String channel, final Object message) {
        this.redisTemplate.convertAndSend(channel, message);
    }

    @Override
    public void flushDB() {
        this.redisTemplate.execute(new RedisCallback<Void>() {
            @Override
            public Void doInRedis(RedisConnection connection) throws DataAccessException {
                connection.flushDb();
                return null;
            }
        });
    }

    @Override
    public long dbSize() {
        return this.redisTemplate.execute(new RedisCallback<Long>() {
            @Override
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return connection.dbSize();
            }
        });
    }

    public String ping() {
        return this.redisTemplate.execute(new RedisCallback<String>() {
            @Override
            public String doInRedis(RedisConnection connection) throws DataAccessException {
                return connection.ping();
            }
        });
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
        this.redisTemplate.execute(new RedisCallback<Void>() {
            @Override
            public Void doInRedis(RedisConnection connection) throws org.springframework.dao.DataAccessException {
                RedisSerializer<Object> serializer = (RedisSerializer<Object>) RedisCache.this.redisTemplate.getKeySerializer();
                for (int i = 0; i < keys.length; i++) {
                    connection.del(serializer.serialize(keys[i]));
                }
                return null;
            }
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
        this.redisTemplate.execute(new RedisCallback<Void>() {
            @Override
            public Void doInRedis(RedisConnection connection) throws org.springframework.dao.DataAccessException {
                @SuppressWarnings("unchecked")
                RedisSerializer<Object> serializer = (RedisSerializer<Object>) RedisCache.this.redisTemplate.getKeySerializer();
                for (Serializable key : keys) {
                    connection.del(serializer.serialize(key));
                }
                return null;
            }
        });
    }

    /**
     * @param pattern 正则表达式
     */
    @Override
    public Set<Serializable> keys(final String pattern) {
        Set<Serializable> result = new HashSet<>(this.redisTemplate.keys(pattern));
        return result;
    }

    @Override
    public <V extends Serializable, K extends Serializable> boolean setNX(K key, V value) {
        return this.redisTemplate.opsForValue().setIfAbsent(key, value);
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
            Cursor<byte[]> cursor = connection.scan(ScanOptions.scanOptions().count(Long.MAX_VALUE).match(pattern).build());
            cursor.forEachRemaining(consumer);
            return null;
        });
    }
}
