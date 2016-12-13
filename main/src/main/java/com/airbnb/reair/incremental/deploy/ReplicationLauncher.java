package com.airbnb.reair.incremental.deploy;

import com.airbnb.reair.db.DbConnectionFactory;
import com.airbnb.reair.db.DbConnectionWatchdog;
import com.airbnb.reair.db.DbKeyValueStore;
import com.airbnb.reair.db.StaticDbConnectionFactory;
import com.airbnb.reair.incremental.DirectoryCopier;
import com.airbnb.reair.incremental.ReplicationServer;
import com.airbnb.reair.incremental.StateUpdateException;
import com.airbnb.reair.incremental.auditlog.AuditLogEntryException;
import com.airbnb.reair.incremental.auditlog.AuditLogReader;
import com.airbnb.reair.incremental.configuration.Cluster;
import com.airbnb.reair.incremental.configuration.ClusterFactory;
import com.airbnb.reair.incremental.configuration.ConfigurationException;
import com.airbnb.reair.incremental.configuration.ConfiguredClusterFactory;
import com.airbnb.reair.incremental.configuration.KrbClusterUtils;
import com.airbnb.reair.incremental.configuration.SecuredClusterFactory;
import com.airbnb.reair.incremental.configuration.TokenInitException;
import com.airbnb.reair.incremental.db.PersistedJobInfoStore;
import com.airbnb.reair.incremental.filter.ReplicationFilter;
import com.airbnb.reair.incremental.thrift.TReplicationService;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ReplicationLauncher {
  private static final Log LOG = LogFactory.getLog(
      ReplicationLauncher.class);

  /**
   * Launches the replication sever process using the passed in configuration.
   *
   * @param conf configuration object
   * @param startAfterAuditLogId instruct the server to start replicating entries after this ID
   * @param resetState if there were jobs that were in progress last time the process exited, do not
   *                   resume them and instead mark them as aborted
   *
   * @throws SQLException if there is an error accessing the DB
   * @throws ConfigurationException if there is an error with the supplied configuration
   * @throws IOException if there is an error communicating with services
   */
  public static void launch(Configuration conf,
      Optional<Long> startAfterAuditLogId,
      boolean resetState)
          throws AuditLogEntryException, ConfigurationException, IOException, StateUpdateException,
          SQLException, TokenInitException {


    // Create the audit log reader
    String auditLogJdbcUrl = conf.get(
        ConfigurationKeys.AUDIT_LOG_JDBC_URL);
    String auditLogDbUser = conf.get(
        ConfigurationKeys.AUDIT_LOG_DB_USER);
    String auditLogDbPassword = conf.get(
        ConfigurationKeys.AUDIT_LOG_DB_PASSWORD);
    DbConnectionFactory auditLogConnectionFactory =
        new StaticDbConnectionFactory(
            auditLogJdbcUrl,
            auditLogDbUser,
            auditLogDbPassword);
    String auditLogTableName = conf.get(
        ConfigurationKeys.AUDIT_LOG_DB_TABLE);
    String auditLogObjectsTableName = conf.get(
        ConfigurationKeys.AUDIT_LOG_OBJECTS_DB_TABLE);
    String auditLogMapRedStatsTableName = conf.get(
        ConfigurationKeys.AUDIT_LOG_MAPRED_STATS_DB_TABLE);

    final AuditLogReader auditLogReader = new AuditLogReader(
        conf,
        auditLogConnectionFactory,
        auditLogTableName,
        auditLogObjectsTableName,
        auditLogMapRedStatsTableName,
        0);

    // Create the connection to the key value store in the DB
    String stateJdbcUrl = conf.get(
        ConfigurationKeys.STATE_JDBC_URL);
    String stateDbUser = conf.get(
        ConfigurationKeys.STATE_DB_USER);
    String stateDbPassword = conf.get(
        ConfigurationKeys.STATE_DB_PASSWORD);
    String keyValueTableName = conf.get(
        ConfigurationKeys.STATE_KV_DB_TABLE);

    DbConnectionFactory stateConnectionFactory =
        new StaticDbConnectionFactory(
            stateJdbcUrl,
            stateDbUser,
            stateDbPassword);

    final DbKeyValueStore dbKeyValueStore = new DbKeyValueStore(
        stateConnectionFactory,
        keyValueTableName);

    String stateTableName = conf.get(
        ConfigurationKeys.STATE_DB_TABLE);

    // Create the store for replication job info
    PersistedJobInfoStore persistedJobInfoStore =
        new PersistedJobInfoStore(
            conf,
            stateConnectionFactory,
            stateTableName);

    if (resetState) {
      LOG.info("Resetting state by aborting non-completed jobs");
      persistedJobInfoStore.abortRunnableFromDb();
    }
//TODO if secured cluster
//    ClusterFactory clusterFactory = new ConfiguredClusterFactory();
    ClusterFactory clusterFactory = new SecuredClusterFactory();
    clusterFactory.setConf(conf);

    tokensInit(new Credentials(), conf);

    final Cluster srcCluster = clusterFactory.getSrcCluster();
    final Cluster destCluster = clusterFactory.getDestCluster();

    String objectFilterClassNames = conf.get(
        ConfigurationKeys.OBJECT_FILTER_CLASS);

    final List<ReplicationFilter> replicationFilters = new ArrayList<>();
    String[] classNames = objectFilterClassNames.split(",");
    for (String objectFilterClassName : classNames) {
      objectFilterClassName = objectFilterClassName.trim().replaceAll("\\r|\\n", "");
      // Instantiate the class
      Object obj = null;
      try {
        Class<?> clazz = Class.forName(objectFilterClassName);
        obj = clazz.newInstance();
        if (!(obj instanceof ReplicationFilter)) {
          throw new ConfigurationException(String.format(
              "%s is not of type %s",
              obj.getClass().getName(),
              ReplicationFilter.class.getName()));
        }
      } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
        throw new ConfigurationException(e);
      }
      ReplicationFilter filter = (ReplicationFilter) obj;
      filter.setConf(conf);
      replicationFilters.add(filter);
    }

    int numWorkers = conf.getInt(
        ConfigurationKeys.WORKER_THREADS,
        1);

    int maxJobsInMemory = conf.getInt(
        ConfigurationKeys.MAX_JOBS_IN_MEMORY,
        100);

    final int thriftServerPort = conf.getInt(
        ConfigurationKeys.THRIFT_SERVER_PORT,
        9996);

    LOG.debug("Running replication server");

    ReplicationServer replicationServer = new ReplicationServer(
        conf,
        srcCluster,
        destCluster,
        auditLogReader,
        dbKeyValueStore,
        persistedJobInfoStore,
        replicationFilters,
        clusterFactory.getDirectoryCopier(),
        numWorkers,
        maxJobsInMemory,
        startAfterAuditLogId);

    // Start thrift server
    final TReplicationService.Processor processor =
        new TReplicationService.Processor<TReplicationService.Iface>(
            replicationServer);

    Runnable serverRunnable = new Runnable() {
      public void run() {
        try {
          TServerTransport serverTransport = new TServerSocket(
              thriftServerPort);
          TServer server = new TSimpleServer(
              new TServer.Args(
                serverTransport).processor(processor));

          LOG.debug("Starting the thrift server on port " + thriftServerPort);
          server.serve();
        } catch (Exception e) {
          LOG.error("Thrift server died!", e);
        }
      }
    };

    Thread serverThread = new Thread(serverRunnable);
    serverThread.start();

    // Start DB connection watchdog - kills the server if a DB connection
    // can't be made.
    DbConnectionWatchdog dbConnectionWatchdog = new DbConnectionWatchdog(
        stateConnectionFactory);
    dbConnectionWatchdog.start();

    // Start replicating entries
    try {
      replicationServer.run(Long.MAX_VALUE);
    } finally {
      LOG.debug("Replication server stopped running");
    }
  }

  private static void tokensInit(Credentials credentials, Configuration config) throws TokenInitException {
    try {
      TokenCache.obtainTokensForNamenodes(
              credentials,
              new Path[] {
                      new Path(config.get(ConfigurationKeys.SRC_HDFS_ROOT)),
                      new Path(config.get(ConfigurationKeys.DEST_HDFS_ROOT)) },
              config);

      URI srcUri = new URI(config.get(ConfigurationKeys.SRC_CLUSTER_METASTORE_URL));
      KrbClusterUtils.initMetastoreDelegationToken(
              srcUri,
              KrbClusterUtils.REAIR_KEY_TOKEN_SIGNATURE_SRC,
              config.get(ConfigurationKeys.SRC_METASTORE_PRINCIPAL),
              credentials
      );

      URI destUri = new URI(config.get(ConfigurationKeys.DEST_CLUSTER_METASTORE_URL));
      KrbClusterUtils.initMetastoreDelegationToken(
              destUri,
              KrbClusterUtils.REAIR_KEY_TOKEN_SIGNATURE_DEST,
              config.get(ConfigurationKeys.DEST_METASTORE_PRINCIPAL),
              credentials
      );
    } catch (Exception ex) {
      throw new TokenInitException(ex);
    }
  }

  /**
   * Launcher entry point.
   *
   * @param argv array of string arguments
   */
  @SuppressWarnings("static-access")
  public static void main(String[] argv)
          throws AuditLogEntryException, ConfigurationException, IOException, ParseException,
          StateUpdateException, SQLException, TokenInitException {
    Options options = new Options();

    options.addOption(OptionBuilder.withLongOpt("config-files")
        .withDescription("Comma separated list of paths to "
            + "configuration files")
        .hasArg()
        .withArgName("PATH")
        .create());

    options.addOption(OptionBuilder.withLongOpt("start-after-id")
        .withDescription("Start processing entries from the audit "
            + "log after this ID")
        .hasArg()
        .withArgName("ID")
        .create());

    CommandLineParser parser = new BasicParser();
    CommandLine cl = parser.parse(options, argv);

    String configPaths = null;
    Optional<Long> startAfterId = Optional.empty();
    boolean resetState = false;

    if (cl.hasOption("config-files")) {
      configPaths = cl.getOptionValue("config-files");
      LOG.info("configPaths=" + configPaths);
    }

    if (cl.hasOption("start-after-id")) {
      startAfterId = Optional.of(
          Long.parseLong(cl.getOptionValue("start-after-id")));
      LOG.info("startAfterId="  + startAfterId);
      resetState = true;
    }

    // Threads shouldn't exit with an exception - terminate to facilitate debugging.
    Thread.setDefaultUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
      LOG.error(String.format("Exiting due to uncaught exception from thread %s!", thread),
          throwable);
      System.exit(-1);
    });

    Configuration conf = new Configuration();

    if (configPaths != null) {
      for (String configPath : configPaths.split(",")) {
        conf.addResource(new Path(configPath));
      }
    }

    try {
      launch(conf, startAfterId, resetState);
    } catch (Exception e) {
      LOG.fatal("Got an exception!", e);
      throw e;
    }
  }
}
