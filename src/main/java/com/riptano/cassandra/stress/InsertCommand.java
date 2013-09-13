package com.riptano.cassandra.stress;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class InsertCommand extends StressCommand {

//    private static final String KEY_FORMAT = "%010d";

    private static final String KEY_FORMAT = "%010d_%s";

    private static Logger log = LoggerFactory.getLogger(InsertCommand.class);
    
    protected final Mutator<String> mutator;
    private final AtomicLong total;

    public InsertCommand(AtomicLong total, int startKey, CommandArgs commandArgs, CommandRunner commandRunner) {
        super(startKey, commandArgs, commandRunner);
        mutator = HFactory.createMutator(commandArgs.keyspace, StringSerializer.get());
        this.total = total;
    }

    @Override
    public Long call() throws Exception {

        String key = null;
        // take into account string formatting for column width
        int colWidth = commandArgs.columnWidth - 9 <= 0 ? 7 : commandArgs.columnWidth -9;
        int keyWidth = commandArgs.keyWidth - 9 <= 0 ? 7 : commandArgs.keyWidth -9;
        int rows = 0;
        log.info("StartKey: {} for thread {}", startKey, Thread.currentThread().getId());

        long cassandraTime = 0;
        String keyRandomPart = RandomStringUtils.random(keyWidth);

        while (rows < commandArgs.getKeysPerThread()) {
            if ( log.isDebugEnabled() ) {
                log.debug("rows at: {} for thread {}", rows, Thread.currentThread().getId());
            }
            int insertsCount = 0;
            for (int j = 0; j < commandArgs.batchSize; j++) {
                key = String.format(KEY_FORMAT, rows+startKey, keyRandomPart);
                for (int j2 = 0; j2 < commandArgs.columnCount; j2++) {
                    mutator.addInsertion(key, commandArgs.workingColumnFamily, HFactory.createStringColumn(String.format(COLUMN_NAME_FORMAT, j2),
                            String.format(COLUMN_VAL_FORMAT, j2, RandomStringUtils.random(colWidth))));
                    insertsCount++;
                    if ( j2 > 0 && j2 % commandArgs.batchSize == 0 ) {
                      executeMutator(mutator, rows);
                    }
                }
                
                if (++rows == commandArgs.getKeysPerThread() ) {
                    break;
                }
            }
            long start = System.currentTimeMillis();
            executeMutator(mutator,rows);
            cassandraTime += (System.currentTimeMillis() - start);

            total.addAndGet(insertsCount);
        }
        commandRunner.doneSignal.countDown();
        log.info("Last key was: {} for thread {}", key, Thread.currentThread().getId());
        // while less than mutationBatchSize,
        // - while less than rowCount
        //   - mutator.insert
        // mutator.execute();
        
        
        log.info("Executed chunk of {}. Latch now at {}", commandArgs.getKeysPerThread(), commandRunner.doneSignal.getCount());
        return cassandraTime;
    }


    
    private static final String COLUMN_VAL_FORMAT = "%08d_%s";
    private static final String COLUMN_NAME_FORMAT = "col_%08d";
}
