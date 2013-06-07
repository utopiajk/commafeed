package com.commafeed.backend.feeds;

import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.commafeed.backend.MetricsBean;
import com.commafeed.backend.dao.FeedDAO;
import com.commafeed.backend.dao.FeedEntryDAO;
import com.commafeed.backend.model.ApplicationSettings;
import com.commafeed.backend.model.Feed;
import com.commafeed.backend.model.FeedEntry;
import com.commafeed.backend.pubsubhubbub.SubscriptionHandler;
import com.commafeed.backend.services.ApplicationSettingsService;
import com.commafeed.backend.services.FeedUpdateService;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Striped;

@Singleton
public class FeedRefreshUpdater {

	protected static Logger log = LoggerFactory
			.getLogger(FeedRefreshUpdater.class);

	@Inject
	FeedUpdateService feedUpdateService;

	@Inject
	FeedRefreshTaskGiver taskGiver;

	@Inject
	FeedDAO feedDAO;

	@Inject
	ApplicationSettingsService applicationSettingsService;

	@Inject
	MetricsBean metricsBean;

	@Inject
	FeedEntryDAO feedEntryDAO;

	@Inject
	SubscriptionHandler subscriptionHandler;

	private ThreadPoolExecutor pool;
	private BlockingQueue<Runnable> queue;
	private Striped<Lock> locks;

	@PostConstruct
	public void init() {
		ApplicationSettings settings = applicationSettingsService.get();
		int threads = Math.max(settings.getDatabaseUpdateThreads(), 1);
		log.info("Creating database pool with {} threads", threads);
		locks = Striped.lazyWeakLock(threads * 100000);
		pool = new ThreadPoolExecutor(threads, threads, 0,
				TimeUnit.MILLISECONDS,
				queue = new ArrayBlockingQueue<Runnable>(500 * threads));
		pool.setRejectedExecutionHandler(new RejectedExecutionHandler() {
			@Override
			public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
				log.debug("Thread queue full, waiting...");
				try {
					e.getQueue().put(r);
				} catch (InterruptedException e1) {
					log.error("Interrupted while waiting for queue.", e);
				}
			}
		});
	}

	@PreDestroy
	public void shutdown() {
		pool.shutdownNow();
		while (!pool.isTerminated()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				log.error("interrupted while waiting for threads to finish.");
			}
		}
	}

	public void updateFeed(Feed feed, List<FeedEntry> entries) {
		pool.execute(new Task(feed, entries));
	}

	private class Task implements Runnable {

		private Feed feed;
		private Collection<FeedEntry> entries;

		public Task(Feed feed, Collection<FeedEntry> entries) {
			this.feed = feed;
			this.entries = entries;
		}

		@Override
		public void run() {
			boolean ok = true;
			if (entries.isEmpty() == false) {
				try {
					updateEntries(feed, entries);
				} catch (Exception e) {
					log.error(e.getMessage(), e);
					ok = false;
				}
			}

			if (!ok) {
				feed.setDisabledUntil(null);
			}

			if (applicationSettingsService.get().isPubsubhubbub()) {
				handlePubSub(feed);
			}

			metricsBean.feedUpdated();
			taskGiver.giveBack(feed);
		}
	}

	private void updateEntries(Feed feed, Collection<FeedEntry> entries)
			throws InterruptedException {
		entries = sortEntries(entries);

		Iterable<Lock> bulkLocks = locks.bulkGet(getKeys(entries));
		List<Lock> lockedLocks = Lists.newArrayList();

		for (Lock lock : bulkLocks) {
			try {
				boolean locked = lock.tryLock(1, TimeUnit.MINUTES);
				if (locked) {
					lockedLocks.add(lock);
				} else {
					throw new InterruptedException("lock timeout for "
							+ feed.getUrl());
				}
			} catch (InterruptedException e) {
				for (Lock l : lockedLocks) {
					l.unlock();
				}
				throw e;
			}
		}

		try {
			feedUpdateService.updateEntries(feed, entries);
		} finally {
			for (Lock l : lockedLocks) {
				l.unlock();
			}
		}
	}

	private List<FeedEntry> sortEntries(Collection<FeedEntry> entries) {
		List<FeedEntry> list = Lists.newArrayList();
		for (FeedEntry entry : entries) {
			list.add(entry);
		}
		Collections.sort(list, new Comparator<FeedEntry>() {
			@Override
			public int compare(FeedEntry o1, FeedEntry o2) {
				String k1 = getKey(o1);
				String k2 = getKey(o2);
				return k1.compareTo(k2);
			}
		});
		return list;
	}

	private List<String> getKeys(Collection<FeedEntry> entries) {
		List<String> keys = Lists.newArrayList();
		for (FeedEntry entry : entries) {
			keys.add(getKey(entry));
		}
		return keys;
	}

	private String getKey(FeedEntry entry) {
		return DigestUtils.sha1Hex(StringUtils.trimToEmpty(entry.getGuid()
				+ entry.getUrl()));
	}

	private void handlePubSub(final Feed feed) {
		Date lastPing = feed.getPushLastPing();
		Date now = Calendar.getInstance().getTime();
		if (lastPing == null || lastPing.before(DateUtils.addDays(now, -3))) {
			new Thread() {
				@Override
				public void run() {
					subscriptionHandler.subscribe(feed);
				}
			}.start();
		}
	}

	public int getQueueSize() {
		return queue.size();
	}

}
