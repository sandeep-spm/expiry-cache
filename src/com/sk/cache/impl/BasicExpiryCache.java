package com.sk.cache.impl;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.sk.cache.ExpiryCache;

/**
 * A Basic Expiry Cache that is good if the expiry intervals are small, and put
 * operations are frequent.
 */
public final class BasicExpiryCache<K, V> implements ExpiryCache<K, V> {

	final Map<K, ValueObject> data;

	// Stores roughly the average time of ttl's which could be used as a sleep
	// time in between the cleanup operations.
	long threadSleepTime;

	public BasicExpiryCache() {
		super();

		/**
		 * Using a concurrentHashmap because it has a good synchronized
		 * implementation.
		 */
		this.data = new ConcurrentHashMap<K, ValueObject>();

		ExecutorService executorService = Executors.newSingleThreadExecutor();

		executorService.submit(new Runnable() {

			@Override
			public void run() {
				while (true) {
					cleanup();

					try {
						Thread.sleep(threadSleepTime);
					} catch (InterruptedException e) {
					}
				}
			}
		});
		executorService.shutdown();
	}

	/**
	 * put this entry(key,value) in the Cache with provided ttl. If cache get
	 * happens within this ttl, value should be returned else cache get should
	 * return null
	 */
	@Override
	public void put(K key, V value, int ttl, TimeUnit timeUnit) {

		if (ttl == 0 || timeUnit == null) {
			return;
		}
		long expiryTime = System.nanoTime() + timeUnit.toNanos(ttl);
		ValueObject valueObj = new ValueObject(value, expiryTime);

		// This is not thread safe. But that is ok. This is just a sleep time
		// for cleanup operation.
		// There would not be any significant benefit of slowing down put
		// operations to accurately calculate threadSleepTime.
		long threadSleepTime = ((this.threadSleepTime * data.size()) + timeUnit
				.toMillis(ttl)) / (data.size() + 1);
		this.threadSleepTime = threadSleepTime;

		// This is threadsafe.
		data.put(key, valueObj);

	}

	/**
	 * get entry value from cache for this key unless that is expired. If it is
	 * expired, it removes the entry
	 * 
	 * @param key
	 * @return value associated with the key
	 */
	@Override
	public V get(K key) {

		ValueObject value = data.get(key);
		if (value != null && value.getExpiryTime() > System.nanoTime()) {
			return value.getData();
		} else {
			remove(key);
			return null;
		}

	}

	/**
	 * removes the given key from the Cache
	 */
	private void remove(K key) {
		data.remove(key);
	}

	private void cleanup() {

		long currentTime = System.nanoTime();
		for (Entry<K, ValueObject> entrySet : data.entrySet()) {
			ValueObject value = entrySet.getValue();
			if (value.getExpiryTime() < currentTime) {
				remove(entrySet.getKey());
			}
			
		}
	}

	private class ValueObject {

		V data;

		// Expiry Time in Nanos
		long expiryTime;

		public ValueObject(V data, long expiryTime) {
			super();
			this.data = data;
			this.expiryTime = expiryTime;
		}

		public V getData() {
			return data;
		}

		public long getExpiryTime() {
			return expiryTime;
		}

	}

}
