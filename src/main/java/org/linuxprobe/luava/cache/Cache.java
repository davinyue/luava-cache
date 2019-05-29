package org.linuxprobe.luava.cache;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

public interface Cache {
	public <V extends Serializable, K extends Serializable> void set(final K key, final V value);

	public <V extends Serializable, K extends Serializable> void set(final K key, final V value, final long timeout);

	public <V extends Serializable, K extends Serializable> V get(final K key);

	@SuppressWarnings("unchecked")
	public <T extends Serializable> void delete(final T... keys);

	public <T extends Serializable> void delete(final Collection<T> keys);

	/** 设置过期时间 */
	public <T extends Serializable> void setExpire(final T key, final long timeout);

	/**
	 * @param pattern key的正则匹配表达式
	 */
	public <T extends Serializable> Set<T> keys(final String pattern);

	/** 如果不存key,则设置值并返回true，存在返回false */
	public <V extends Serializable, K extends Serializable> boolean setNX(final K key, final V value);

	/** 设置新值返回旧值 */
	public <V extends Serializable, K extends Serializable> Serializable getAndSet(final K key, final V value);

	/** 清空所有数据 */
	public void flushDB();

	/** 返回kes数量 */
	public long dbSize();
}
