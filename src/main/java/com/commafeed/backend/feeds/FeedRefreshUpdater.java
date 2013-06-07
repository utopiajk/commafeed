package com.commafeed.backend.feeds;

import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.commafeed.backend.MetricsBean;
import com.commafeed.backend.dao.FeedDAO;
import com.commafeed.backend.dao.FeedEntryDAO;
import com.commafeed.backend.model.Feed;
import com.commafeed.backend.model.FeedEntry;
import com.commafeed.backend.pubsubhubbub.SubscriptionHandler;
import com.commafeed.backend.services.ApplicationSettingsService;
import com.commafeed.backend.services.FeedUpdateService;

@Singleton
public class FeedRefreshUpdater {

	protected static Logger log = LoggerFactory
			.getLogger(FeedRefreshUpdater.class);

	@Inject
	FeedUpdateService feedUpdateService;

	@Inject
	SubscriptionHandler handler;

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

	private ThreadPoolExecutor pool;
	private BlockingQueue<Runnable> queue;

	@PostConstruct
	public void init() {
		pool = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
				queue = new ArrayBlockingQueue<Runnable>(10000));
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

	public void updateFeed(Feed feed, Collection<FeedEntry> entries) {
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
					feedUpdateService.updateEntries(feed, entries);
				} catch (Exception e) {
					log.error(e.getMessage(), e);
					ok = false;
				}
			}

			if (applicationSettingsService.get().isPubsubhubbub()) {
				handlePubSub(feed);
			}
			if (!ok) {
				feed.setDisabledUntil(null);
			}
			metricsBean.feedUpdated();
			taskGiver.giveBack(feed);
		}
	}

	private void handlePubSub(final Feed feed) {
		if (feed.getPushHub() != null && feed.getPushTopic() != null) {
			Date lastPing = feed.getPushLastPing();
			Date now = Calendar.getInstance().getTime();
			if (lastPing == null || lastPing.before(DateUtils.addDays(now, -3))) {
				new Thread() {
					@Override
					public void run() {
						handler.subscribe(feed);
					}
				}.start();
			}
		}
	}

	public int getQueueSize() {
		return queue.size();
	}

}
