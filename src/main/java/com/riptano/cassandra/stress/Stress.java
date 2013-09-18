package com.riptano.cassandra.stress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import jline.ConsoleReader;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initiate a stress run against an Apache Cassandra cluster
 *
 * @author zznate <nate@riptano.com>
 */
public class Stress {

    public static final String REPLICATION_FACTOR = "replication-factor";
    public static final String DISABLE_DURABLE_WRITES = "disable-durable-writes";
    public static final String KEY_WIDTH = "key-width";
    public static final String REUSE_KEYSPACE = "reuse-keyspace";
    public static final String CF_PERF_THREAD = "cf-perf-thread";
    public static final String COLUMN_FAMILY_NAME = "column-family-name";
    private static Logger log = LoggerFactory.getLogger(Stress.class);
    
    private CommandArgs commandArgs;
    private CommandRunner commandRunner;
    private static String seedHost;
    
    public static void main( String[] args ) throws Exception {
        Stress stress = new Stress();
        CommandLine cmd = stress.processArgs(args);
        // If we got an initial help, leave
        if ( cmd.hasOption("help") || cmd.getArgList().size() == 0 ) {
            printHelp(true);
            System.exit(0);
        }
        seedHost = cmd.getArgList().size() > 1 ? cmd.getArgs()[1] : cmd.getArgs()[0];
        
        log.info("Starting stress run using seed {} for {} clients...", seedHost, stress.commandArgs.threads);
        try {
            stress.initializeCommandRunner(cmd);
        } catch (IllegalArgumentException iae) {
            log.error("Could not run command:",iae);
            printHelp(true);
            System.exit(0);
        }
        
        ConsoleReader reader = new ConsoleReader();
        String line;
        while ((line = reader.readLine("[cassandra-stress] ")) != null) {
            if ( line.equalsIgnoreCase("exit")) {
                System.exit(0);
            }
            stress.processCommand(reader, line);
        }
    }
    
    private void processCommand(ConsoleReader reader, String line) throws Exception {
        // TODO catch command error(s) here, simply errmsg handoff to stdin loop above
        CommandLine cmd = processArgs(line.split(" "));
        if (cmd.hasOption("help")) 
            return;
        if ( commandArgs.validateCommand() ) {
            commandRunner.processCommand(commandArgs);            
        } else {
            reader.printString("Invalid command. Must be one of: read, rangeslice, multiget\n");
            printHelp(false);
        }        
    }
    
    
    private CommandLine processArgs(String[] args) throws Exception {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse( buildOptions(), args);
        if ( cmd.hasOption("help")) {
            printHelp(false);
            return cmd;
        }
        if ( commandArgs == null ) {
            commandArgs = new CommandArgs();
        }
        
        if (cmd.hasOption("threads")) {
            commandArgs.threads = getIntValueOrExit(cmd, "threads");
        }
        
        if (cmd.hasOption("clients")) {
            commandArgs.clients = getIntValueOrExit(cmd, "clients");
        }
        
        if (cmd.hasOption("start-key")) {
          commandArgs.startKey = getIntValueOrExit(cmd, "start-key");
        }
         
            
        if ( cmd.hasOption("num-keys") ) {
            commandArgs.rowCount = getIntValueOrExit(cmd, "num-keys");
        }

        if ( cmd.hasOption("batch-size")) {
            commandArgs.batchSize = getIntValueOrExit(cmd, "batch-size");
        }

        if ( cmd.hasOption("columns")) {
            commandArgs.columnCount = getIntValueOrExit(cmd, "columns");
        }
        
        if ( cmd.hasOption("colwidth")) {
            commandArgs.columnWidth = getIntValueOrExit(cmd, "colwidth");
        }

        if ( cmd.hasOption(KEY_WIDTH)) {
            commandArgs.keyWidth = getIntValueOrExit(cmd, KEY_WIDTH);
        }


        if (cmd.hasOption("operation")) {            
            commandArgs.operation = cmd.getOptionValue("operation"); 
        } else {
            // reset args from no-arg if we have one
            commandArgs.operation = cmd.getArgList().size() > 0 ? cmd.getArgs()[0] : commandArgs.operation;
        }
        Operation actOpt;
        try {
            actOpt = commandArgs.getOperation();        
        } catch (IllegalArgumentException iae) {
            return cmd;
        }
        if ( actOpt == Operation.   REPLAY ) {
            try {
                commandArgs.replayCount = cmd.getArgList().size() > 1 ? Integer.valueOf(cmd.getArgs()[1]) : 1;
            } catch (NumberFormatException nfe) {
                throw new IllegalArgumentException("The replay command can only take a resonably sized number as an (optional) argument");
            }
        }
                        
        log.info("{} {} columns into {} keys in batches of {} from {} threads",
                new Object[]{commandArgs.operation, commandArgs.columnCount, commandArgs.rowCount, 
                commandArgs.batchSize, commandArgs.threads});
                               
        return cmd;
    }
    
    private void initializeCommandRunner(CommandLine cmd) throws Exception {

        CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator(seedHost);
        
        if ( cmd.hasOption("unframed")) {
            cassandraHostConfigurator.setUseThriftFramedTransport(false);
        }
        if (cmd.hasOption("max-wait")) {
            cassandraHostConfigurator.setMaxWaitTimeWhenExhausted(getIntValueOrExit(cmd, "max-wait"));
        }        
        if (cmd.hasOption("thrift-timeout")) {
            cassandraHostConfigurator.setCassandraThriftSocketTimeout(getIntValueOrExit(cmd, "thrift-timeout"));
        }            
        cassandraHostConfigurator.setMaxActive(commandArgs.clients);
        
        if (cmd.hasOption("discovery-delay")) {
            cassandraHostConfigurator.setAutoDiscoverHosts(true);
            cassandraHostConfigurator.setAutoDiscoveryDelayInSeconds(getIntValueOrExit(cmd, "discovery-delay"));
        }
        if (cmd.hasOption("retry-delay")) {          
            cassandraHostConfigurator.setRetryDownedHostsDelayInSeconds(getIntValueOrExit(cmd, "retry-delay"));
        } 
        if (cmd.hasOption("skip-retry-delay")) {          
            cassandraHostConfigurator.setRetryDownedHosts(false);
        }

        boolean reuseKeyspace = cmd.hasOption(REUSE_KEYSPACE);

        ConfigurableConsistencyLevel clc = null;
        if ( cmd.hasOption("consistency-levels")) {
            String[] levels = cmd.getOptionValues("consistency-levels")[0].split(":");
            clc = new ConfigurableConsistencyLevel();                        
            try {
              clc.setDefaultReadConsistencyLevel(HConsistencyLevel.valueOf(levels[0]));
              clc.setDefaultWriteConsistencyLevel(HConsistencyLevel.valueOf(levels[1]));
            } catch (Exception e) {
              throw new IllegalArgumentException("ConsistencyLevels must be specified by their full names. Ie. ONE,QUORUM. " + levels[0]);
            }
        }

        int replicationFactor = cmd.hasOption(REPLICATION_FACTOR) ? getIntValueOrExit(cmd, REPLICATION_FACTOR) : 1;

        boolean durableWrites = !cmd.hasOption(DISABLE_DURABLE_WRITES);

        commandArgs.cfPerThread = cmd.hasOption(CF_PERF_THREAD);

        if (cmd.hasOption(COLUMN_FAMILY_NAME)) {
            if(cmd.hasOption(CF_PERF_THREAD)) {
                throw new IllegalArgumentException("Can not have name and be per thread");
            }
            commandArgs.singleCFName = cmd.getOptionValue(COLUMN_FAMILY_NAME);
        }

        Cluster cluster = HFactory.createCluster("StressCluster", cassandraHostConfigurator);

        // Populate schema if needed.
        KeyspaceDefinition ksDef = cluster.describeKeyspace(commandArgs.workingKeyspace);
        if(ksDef != null && !reuseKeyspace) {
            try {
                cluster.dropKeyspace(commandArgs.workingKeyspace, true);
            } catch (HectorException e) {
                log.warn("Couldn't drop keyspace", e);
            }
        }

        if (!reuseKeyspace || ksDef == null) {
            List<ColumnFamilyDefinition> cfDefs = new ArrayList<ColumnFamilyDefinition>();
            if(commandArgs.cfPerThread) {
                for (int i = 0; i < commandArgs.threads; i++) {
                    String cfName = commandArgs.getWorkingColumnFamily(commandArgs.getKeysPerThread() * i);
                    ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(
                            commandArgs.workingKeyspace, cfName, ComparatorType.BYTESTYPE);
                    cfDefs.add(cfDef);
                }
            } else {
                ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(
                    commandArgs.workingKeyspace, commandArgs.singleCFName, ComparatorType.BYTESTYPE);
                cfDefs.add(cfDef);
            }

            ThriftKsDef newKeyspace = new ThriftKsDef(
                commandArgs.workingKeyspace, ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor, cfDefs);
            newKeyspace.setDurableWrites(durableWrites);

            cluster.addKeyspace(newKeyspace, true);
        }

        commandArgs.keyspace = clc == null ? HFactory.createKeyspace(commandArgs.workingKeyspace, cluster) : 
          HFactory.createKeyspace(commandArgs.workingKeyspace, cluster, clc);
        commandRunner = new CommandRunner(cluster.getKnownPoolHosts(true));


        if ( commandArgs.validateCommand() && commandArgs.getOperation() != Operation.REPLAY) {
            commandRunner.processCommand(commandArgs);
        } else {
            throw new IllegalArgumentException("command: " + commandArgs.getOperation().toString());
        }
    }

    
    // TODO if --use-all-hosts, then buildHostsFromRing()
    // treat the host as a single arg, init cluster and call addHosts for the ring
    
    
    private static Options buildOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "Print this help message and exit");
        options.addOption("o","operation", true, "The type of operation: insert or select");
        options.addOption("t","threads", true, "The number of client threads we will create");
        options.addOption("n","num-keys",true,"The number of keys to create");
        options.addOption("c","columns",true,"The number of columns to create per key");
        options.addOption("C","clients",true,"The number of pooled clients to use");
        options.addOption("b","batch-size",true,"The number of rows in the batch_mutate call");        
        options.addOption("m","unframed",false,"Disable use of TFramedTransport");
        options.addOption("w","colwidth",true,"The width of the column in bytes. Default is 16");
        options.addOption("M","max-wait",true,"The Maximum time to wait on acquiring a connection from the pool (maxWaitTimeWhenExhausted). Default is forever.");
        options.addOption("T","thrift-timeout",true,"The ThriftSocketTimeout value.");
        options.addOption("D","discovery-delay",true,"The amount of time to wait between runs of Auto host discovery. Providing a value enables this service");
        options.addOption("R","retry-delay",true,"The amount of time to wait between runs of Downed host retry delay execution. 30 seconds by default.");
        options.addOption("S","skip-retry-delay",false,"Disable downed host retry service execution.");
        options.addOption("L","consistency-levels",true,"Defaults to QUORUM for R+W, specified in the form of [read]:[write] eg. '-L ONE:ONE'");
        options.addOption("k","start-key",true,"Start on a specific key");
        options.addOption("r", REPLICATION_FACTOR,true,"Replication factor");
        options.addOption("d", DISABLE_DURABLE_WRITES, false, "Disable durable writes(commit log)");
        options.addOption("kw", KEY_WIDTH, true, "Size of a key");
        options.addOption("rk", REUSE_KEYSPACE, false, "Reuse existing keyspace");
        options.addOption("cft", CF_PERF_THREAD, false, "Each thread will use own column family");
        options.addOption("cfn", COLUMN_FAMILY_NAME, true, "Name of column family");
        return options;
    }
    
    private static void printHelp(boolean withInit) {
        HelpFormatter formatter = new HelpFormatter();
        if (withInit) {
            formatter.printHelp( "stress [options]... operation url1,[[url2],[url3],...]", buildOptions() );
        } else {
            formatter.printHelp( "operation [options]", buildOptions() );
        }
    }
    
    private static int getIntValueOrExit(CommandLine cmd, String optionVal) {
        try {
            return Integer.valueOf(cmd.getOptionValue(optionVal));            
        } catch (NumberFormatException ne) {
            log.error("Invalid number of {} provided - must be a reasonably sized positive integer", optionVal);
            System.exit(0);
        }         
        return 0;
    }
}
