package org.linuxprobe.luava.cache;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public interface Cache {
    /**
     * 设置缓存, 永不过期
     *
     * @param key   key
     * @param value valeu
     */
    public <V extends Serializable, K extends Serializable> void set(final K key, final V value);

    /**
     * 设置key
     *
     * @param key     key
     * @param value   valeu
     * @param timeout 有效时长, 单位秒
     */
    public <V extends Serializable, K extends Serializable> void set(final K key, final V value, final long timeout);

    /**
     * 设置缓存
     *
     * @param key      key
     * @param value    valeu
     * @param timeout  有效时长
     * @param timeUnit 时间单位
     */
    public <V extends Serializable, K extends Serializable> void set(final K key, final V value, final long timeout, TimeUnit timeUnit);

    /**
     * 获取key
     */
    public <V extends Serializable, K extends Serializable> V get(final K key);

    @SuppressWarnings("unchecked")
    public <T extends Serializable> void delete(final T... keys);

    public <T extends Serializable> void delete(final Collection<T> keys);

    /**
     * 设置过期时间
     */
    public <T extends Serializable> void setExpire(final T key, final long timeout);

    /**
     * @param pattern key的正则匹配表达式
     */
    public <T extends Serializable> Set<T> keys(final String pattern);

    /**
     * 如果不存key,则设置值并返回true，存在返回false
     */
    public <V extends Serializable, K extends Serializable> boolean setNX(final K key, final V value);

    /**
     * 设置新值返回旧值
     */
    public <V extends Serializable, K extends Serializable> Serializable getAndSet(final K key, final V value);

    /**
     * 清空所有数据
     */
    public void flushDB();

    /**
     * 返回kes数量
     */
    public long dbSize();
}
