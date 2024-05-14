package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.regex.Pattern;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
    private final Clock clock;
    private final Duration timeout;
    private final int popularWordCount;
    private final int maxDepth;
    private final ForkJoinPool pool;
    private final PageParserFactory parserFactory;
    private final List<Pattern> ignoredUrls;

    @Inject
    ParallelWebCrawler(
            Clock clock,
            @Timeout Duration timeout,
            @PopularWordCount int popularWordCount,
            @MaxDepth int maxDepth,
            @TargetParallelism int threadCount,
            PageParserFactory parserFactory,
            @IgnoredUrls List<Pattern> ignoredUrls) {
        this.clock = clock;
        this.timeout = timeout;
        this.popularWordCount = popularWordCount;
        this.maxDepth = maxDepth;
        this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
        this.parserFactory = parserFactory;
        this.ignoredUrls = ignoredUrls;
    }

    @Override
    public CrawlResult crawl(List<String> startingUrls) {
        Instant deadline = clock.instant().plus(timeout);
        ConcurrentMap<String, Integer> counts = new ConcurrentHashMap<>();
        ConcurrentSkipListSet<String> visitedUrls = new ConcurrentSkipListSet<>();

        //invoke tasks
        List<CrawlInternal> tasks = new ArrayList<>();
        for (String url : startingUrls) {
            tasks.add(new CrawlInternal(url, deadline, maxDepth, counts, visitedUrls));
        }
        ForkJoinTask.invokeAll(tasks);

        if (counts.isEmpty()) {
            return new CrawlResult.Builder()
                    .setWordCounts(counts)
                    .setUrlsVisited(visitedUrls.size())
                    .build();
        }

        return new CrawlResult.Builder()
                .setWordCounts(WordCounts.sort(counts, popularWordCount))
                .setUrlsVisited(visitedUrls.size())
                .build();
    }

    @Override
    public int getMaxParallelism() {
        return Runtime.getRuntime().availableProcessors();
    }

    public class CrawlInternal extends RecursiveAction {
        private final String url;
        private final Instant deadline;
        private final int maxDepth;
        private final ConcurrentMap<String, Integer> counts;
        private final ConcurrentSkipListSet<String> visitedUrls;

        public CrawlInternal(String url, Instant deadline, int maxDepth, ConcurrentMap<String, Integer> counts, ConcurrentSkipListSet<String> visitedUrls) {
            this.url = url;
            this.deadline = deadline;
            this.maxDepth = maxDepth;
            this.counts = counts;
            this.visitedUrls = visitedUrls;
        }

        @Override
        protected void compute() {
            if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
                return;
            }
            for (Pattern pattern : ignoredUrls) {
                if (pattern.matcher(url).matches()) {
                    return;
                }
            }
            //atomic operation
            if (!visitedUrls.add(url)) {
                return;
            }
            PageParser.Result result = parserFactory.get(url).parse();

            for (ConcurrentMap.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
                /*if (counts.containsKey(e.getKey())) {
                    counts.put(e.getKey(), e.getValue() + counts.get(e.getKey()));
                } else {
                    counts.put(e.getKey(), e.getValue());
                }*/

                //atomic operation
                counts.compute(e.getKey(), (k, v) -> (v == null) ? e.getValue() : e.getValue() + 1);
            }

            //invoke tasks
            List<CrawlInternal> tasks = new ArrayList<>();
            for (String link : result.getLinks()) {
                tasks.add(new CrawlInternal(link, deadline, maxDepth - 1, counts, visitedUrls));
            }
            ForkJoinTask.invokeAll(tasks);
        }
    }
}
