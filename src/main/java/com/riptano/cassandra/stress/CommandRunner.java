package com.riptano.cassandra.stress;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import me.prettyprint.cassandra.service.CassandraHost;

import org.apache.cassandra.utils.LatencyTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Segments the execution from the command implementation. Also holds
 * the CountDownLatch and latency tracker structures for tracking 
 * progress and statistics
 *  
 * @author zznate <nate@riptano.com>
 */
public class CommandRunner {

    private static final Logger log = LoggerFactory.getLogger(CommandRunner.class);

    public static final long SLEEP_MILLIS = 5000;
    public static final long SLEEP_SECS = TimeUnit.MILLISECONDS.toSeconds(SLEEP_MILLIS);

    final Map<CassandraHost, LatencyTracker> latencies;
    CountDownLatch doneSignal;
    private Operation previousOperation;
    
    public CommandRunner(Set<CassandraHost> cassandraHosts) {
        latencies = new ConcurrentHashMap<CassandraHost, LatencyTracker>();
        for (CassandraHost host : cassandraHosts) {
            latencies.put(host, new LatencyTracker());
        }        
    }
    
    
    public void processCommand(CommandArgs commandArgs) throws Exception {
        if ( commandArgs.getOperation() != Operation.REPLAY ) {
            previousOperation = commandArgs.getOperation();
        }

        ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(commandArgs.threads));
        log.info("Starting command run at {}", new Date());
        long currentTime = System.currentTimeMillis();
        AtomicLong total = new AtomicLong();
        doneSignal = new CountDownLatch(commandArgs.threads);
        List<ListenableFuture<Long>> futures = new ArrayList<ListenableFuture<Long>>(commandArgs.threads);

        long submitTime = System.currentTimeMillis();
        for (int i = 0; i < commandArgs.threads; i++) {
            ListenableFuture<Long> submit = exec.submit(getCommandInstance(total, i * commandArgs.getKeysPerThread(), commandArgs, this));
            futures.add(submit);
        }

        log.info("Submitted {} tasks", commandArgs.threads);

        ListenableFuture<List<Long>> future = Futures.allAsList(futures);
        long previous = 0;
        while(!future.isDone()) {
            long time = System.currentTimeMillis();
            long timeSpentSecs = (time - submitTime) / 1000;
            long currentTotal = total.get();

            long rate = currentTotal / (timeSpentSecs != 0 ? timeSpentSecs : 1);

            long currentRate = (currentTotal - previous) / SLEEP_SECS;
            previous = currentTotal;
            log.info("{} sec: progress {}/{}. Current rate: {} recs/sec, Avg rate: {} recs/sec", new Object[]{timeSpentSecs, currentTotal, commandArgs.rowCount, currentRate, rate});

            try {
                Thread.sleep(SLEEP_MILLIS);
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for work to be finished");
            }
        }

        List<Long> times = future.get();
        long cassandraTime = 0;
        for (Long time : times) {
            if(time != null && time > cassandraTime) {
                cassandraTime = time;
            }
        }

        log.info("Finished command run at {} total duration: {} seconds, cassandra time: {}", new Object[]{new Date(), (System.currentTimeMillis()-currentTime)/1000, cassandraTime});

        exec.shutdown();
        
        for (CassandraHost host : latencies.keySet()) {
            LatencyTracker latency = latencies.get(host);
            log.info("Latency for host {}:\n Op Count {} \nRecentLatencyHistogram {} \nRecent Latency Micros {} \nTotalLatencyHistogram {} \nTotalLatencyMicros {}", 
                    new Object[]{host.getName(), latency.getOpCount(), latency.getRecentLatencyHistogramMicros(), latency.getRecentLatencyMicros(),
                    latency.getTotalLatencyHistogramMicros(), latency.getTotalLatencyMicros()});
        }        
    }
    
    private StressCommand getCommandInstance(AtomicLong counter, int startKeyArg, CommandArgs commandArgs, CommandRunner commandRunner) {
        
        int startKey = commandArgs.startKey + startKeyArg;
        if ( log.isDebugEnabled() ) {
          log.debug("Command requested with starting key pos {} and op {}", startKey, commandArgs.getOperation());
        }
        
        Operation operation = commandArgs.getOperation();
        if ( operation.equals(Operation.REPLAY )) {
            operation = previousOperation;
        }
        switch(operation) {
            case INSERT:
                return new InsertCommand(counter, startKey, commandArgs, commandRunner);
            case READ:
                return new SliceCommand(startKey, commandArgs, commandRunner);
            case RANGESLICE:
                return new RangeSliceCommand(startKey, commandArgs, commandRunner);
            case MULTIGET:
                return new MultigetSliceCommand(startKey, commandArgs, commandRunner);
            case VERIFY_LAST_INSERT:
              return new VerifyLastInsertCommand(startKey, commandArgs, commandRunner);
            case COUNTERSPREAD:
              return new BucketingCounterSpreadCommand(startKey, commandArgs, commandRunner);
        }

        return new InsertCommand(counter, startKey, commandArgs, commandRunner);
    }

}
