package org.linuxprobe.luava.cache.impl;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.linuxprobe.luava.cache.Cache;

public class DefaultCache implements Cache {
	private Map<Serializable, Serializable> keyMapValue = new ConcurrentHashMap<>();
	private Map<Serializable, Long> keyMapTimeout = new ConcurrentHashMap<>();

	/** 判断key是否有效 */
	private boolean isEffective(Serializable key) {
		boolean result = true;
		Long timeOut = this.keyMapTimeout.get(key);
		if (timeOut == null) {
			result = false;
		} else {
			if (timeOut != 0) {
				if (System.currentTimeMillis() >= this.keyMapTimeout.get(key)) {
					this.keyMapValue.remove(key);
					this.keyMapTimeout.remove(key);
					result = false;
				}
			}
		}
		return result;
	}

	@Override
	public <V extends Serializable, K extends Serializable> void set(K key, V value, long timeout) {
		if (key == null) {
			throw new IllegalArgumentException("key cannot be null");
		}
		if (value == null) {
			throw new IllegalArgumentException("value cannot be null");
		}
		if (timeout < 0) {
			throw new IllegalArgumentException("timeout cannot be less than 0");
		}
		this.keyMapValue.put(key, value);
		if (timeout == 0) {
			this.keyMapTimeout.put(key, 0L);
		}
		this.keyMapTimeout.put(key, System.currentTimeMillis() + timeout * 1000);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V extends Serializable, K extends Serializable> V get(K key) {
		V result = null;
		if (isEffective(key)) {
			result = (V) this.keyMapValue.get(key);
		}
		return result;
	}

	@Override
	public <V extends Serializable, K extends Serializable> void set(K key, V value) {
		this.set(key, value);
	}

	@Override
	public <T extends Serializable> void delete(@SuppressWarnings("unchecked") T... keys) {
		if (keys != null) {
			for (Serializable key : keys) {
				this.keyMapValue.remove(key);
				this.keyMapTimeout.remove(key);
			}
		}
	}

	@Override
	public <T extends Serializable> void delete(Collection<T> keys) {
		if (keys != null) {
			for (Serializable key : keys) {
				this.keyMapValue.remove(key);
				this.keyMapTimeout.remove(key);
			}
		}
	}

	@Override
	public <T extends Serializable> void setExpire(T key, long timeout) {
		if (timeout < 0) {
			throw new IllegalArgumentException("timeout cannot be less than 0");
		}
		if (timeout == 0) {
			this.keyMapTimeout.put(key, 0L);
		}
		this.keyMapTimeout.put(key, System.currentTimeMillis() + timeout * 1000);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Serializable> Set<T> keys(String pattern) {
		Set<Serializable> existKeys = this.keyMapValue.keySet();
		Set<T> result = new HashSet<T>();
		if (existKeys != null) {
			for (Serializable existKey : existKeys) {
				if (isEffective(existKey) && existKey.toString().matches(pattern)) {
					result.add((T) existKey);
				}
			}
		}
		return result;
	}

	@Override
	public synchronized <V extends Serializable, K extends Serializable> boolean setNX(K key, V value) {
		if (!isEffective(key)) {
			this.set(key, value);
			return true;
		}
		return false;
	}

	@Override
	public <V extends Serializable, K extends Serializable> Serializable getAndSet(K key, V value) {
		V existValue = get(key);
		set(key, value);
		return existValue;
	}

	@Override
	public synchronized void flushDB() {
		this.keyMapTimeout.clear();
		this.keyMapValue.clear();
	}

	@Override
	public long dbSize() {
		long result = 0;
		Set<Serializable> existKeys = this.keyMapValue.keySet();
		if (existKeys != null) {
			for (Serializable existKey : existKeys) {
				if (isEffective(existKey)) {
					result++;
				}
			}
		}
		return result;
	}
}
