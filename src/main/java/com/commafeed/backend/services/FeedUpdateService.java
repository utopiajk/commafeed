package com.commafeed.backend.services;

import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.ejb.Stateless;
import javax.inject.Inject;

import com.commafeed.backend.MetricsBean;
import com.commafeed.backend.dao.FeedEntryDAO;
import com.commafeed.backend.dao.FeedEntryStatusDAO;
import com.commafeed.backend.dao.FeedSubscriptionDAO;
import com.commafeed.backend.feeds.FeedUtils;
import com.commafeed.backend.model.Feed;
import com.commafeed.backend.model.FeedEntry;
import com.commafeed.backend.model.FeedEntryContent;
import com.commafeed.backend.model.FeedEntryStatus;
import com.commafeed.backend.model.FeedSubscription;
import com.google.api.client.util.Maps;
import com.google.common.collect.Lists;

@Stateless
public class FeedUpdateService {

	@Inject
	FeedSubscriptionDAO feedSubscriptionDAO;

	@Inject
	FeedEntryDAO feedEntryDAO;

	@Inject
	FeedEntryStatusDAO feedEntryStatusDAO;

	@Inject
	MetricsBean metricsBean;

	public void updateEntries(Feed feed, Collection<FeedEntry> entries) {
		List<String> guids = Lists.newArrayList();
		for (FeedEntry entry : entries) {
			guids.add(entry.getGuid());
		}
		List<FeedEntry> existingEntries = feedEntryDAO.findByGuids(guids);

		Map<FeedEntry, FeedEntry> map = Maps.newHashMap();
		for (FeedEntry entry : entries) {
			FeedEntry existing = FeedUtils.findEntry(existingEntries, entry);
			map.put(entry, existing);
		}

		List<FeedSubscription> subscriptions = feedSubscriptionDAO
				.findByFeed(feed);
		for (FeedEntry entry : entries) {
			updateEntry(feed, entry, map.get(entry), subscriptions);
		}

	}

	public void updateEntry(Feed feed, FeedEntry entry,
			FeedEntry existingEntry, List<FeedSubscription> subscriptions) {

		FeedEntry update = null;
		if (existingEntry == null) {
			FeedEntryContent content = entry.getContent();
			content.setTitle(FeedUtils.truncate(
					FeedUtils.handleContent(content.getTitle(), feed.getLink()),
					2048));
			content.setContent(FeedUtils.handleContent(content.getContent(),
					feed.getLink()));

			entry.setInserted(Calendar.getInstance().getTime());
			entry.getFeeds().add(feed);

			update = entry;
		} else if (FeedUtils.findFeed(existingEntry.getFeeds(), feed) == null) {
			existingEntry.getFeeds().add(feed);
			update = existingEntry;
		}

		if (update != null) {
			List<FeedEntryStatus> statusUpdateList = Lists.newArrayList();
			for (FeedSubscription sub : subscriptions) {
				FeedEntryStatus status = new FeedEntryStatus();
				status.setEntry(update);
				status.setSubscription(sub);
				statusUpdateList.add(status);
			}
			feedEntryDAO.saveOrUpdate(update);
			feedEntryStatusDAO.saveOrUpdate(statusUpdateList);
			metricsBean.entryUpdated(statusUpdateList.size());
		}
	}

}
