package com.sk.cache.impl;

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.sk.cache.ExpiryCache;

/**
 * An Expiry Cache implementation with a Priority Queue Based eviction. Ideally
 * good if Get operations out-number puts, or if the expiry Intervals are large.
 * But seems to be easily outperformed by {@code BasicExpiryCache}
 */
public class QueuedExpiryCache<K, V> implements ExpiryCache<K, V> {

	final Map<K, ValueObject> data;

	PriorityQueue<ExpiryObject> evictionQueue;

	long threadSleepTime;

	public QueuedExpiryCache() {
		super();
		this.data = new ConcurrentHashMap<K, ValueObject>();
		this.evictionQueue = new PriorityQueue<>(10,
				new ExpiryObjectComparator());
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

	private synchronized void cleanup() {
		long curentTime = System.nanoTime();
		while (!evictionQueue.isEmpty()) {
			ExpiryObject expiryObjectHead = evictionQueue.peek();
			if (expiryObjectHead != null
					&& expiryObjectHead.getEvictionTimeInNanos() < curentTime) {
				expiryObjectHead = evictionQueue.poll();
				remove(expiryObjectHead.getKey());
			} else {
				break;
			}
		}
	}

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

		data.put(key, valueObj);
		synchronized (data) {
			evictionQueue.add(new ExpiryObject(key, expiryTime));
		}

	}

	/**
	 * get entry value from cache for this key unless that is expired. If it is
	 * expired, it removes the entry
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

	private class ExpiryObject {

		K key;
		long evictionTimeInNanos;

		public ExpiryObject(K key, long evictionTimeInNanos) {
			super();
			this.key = key;
			this.evictionTimeInNanos = evictionTimeInNanos;
		}

		@Override
		public String toString() {
			return "ExpiryObject [key=" + key + ", evictionTimeInNanos="
					+ evictionTimeInNanos + "]";
		}

		public K getKey() {
			return key;
		}

		public long getEvictionTimeInNanos() {
			return evictionTimeInNanos;
		}

	}

	private class ExpiryObjectComparator implements Comparator<ExpiryObject> {

		@Override
		public int compare(ExpiryObject o1, ExpiryObject o2) {
			if (o1 != null && o2 != null) {
				if (o1.getEvictionTimeInNanos() > o2.getEvictionTimeInNanos()) {
					return 1;
				} else if (o1.getEvictionTimeInNanos() < o2
						.getEvictionTimeInNanos()) {
					return -1;
				}
			}
			return 0;
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
