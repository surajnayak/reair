package com.airbnb.reair.common;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Table;

import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

/**
 * Concrete implementation of secured HiveMetastoreClient.
 */
public class KrbHiveMetastoreClient implements HiveMetastoreClient {

  private IMetaStoreClient client;

  public KrbHiveMetastoreClient(IMetaStoreClient client) {
    this.client = client;
  }

  /**
   * TODO.
   */
  public void close() {

  }

  private void connectIfNeeded() throws HiveMetastoreException {

  }

  /**
   * TODO.
   *
   * @param partition TODO
   * @return TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public synchronized Partition addPartition(Partition partition) throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.add_partition(partition);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  /**
   * TODO.
   *
   * @param dbName TODO
   * @param tableName TODO
   * @return TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public synchronized Table getTable(String dbName, String tableName)
      throws HiveMetastoreException {

    try {
      connectIfNeeded();

      Table table = client.getTable(dbName, tableName);
      return table;
    } catch (NoSuchObjectException e) {
      return null;
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public PrincipalPrivilegeSet listTablePrivileges(String dbName, String tableName, String owner) throws HiveMetastoreException {
    try {
      PrincipalPrivilegeSet principalPrivs = new PrincipalPrivilegeSet();
      principalPrivs.setUserPrivileges(createGrants(dbName, tableName, owner, PrincipalType.USER));
      principalPrivs.setGroupPrivileges(createGrants(dbName, tableName, owner, PrincipalType.GROUP));
      principalPrivs.setRolePrivileges(createGrants(dbName, tableName, owner, PrincipalType.ROLE));
      return  principalPrivs;
    } catch (TException ex) {
      close();
      throw new HiveMetastoreException(ex);
    }
  }

  private Map<String, List<PrivilegeGrantInfo>> createGrants(String dbName, String tableName, String owner, PrincipalType principalType) throws TException {
    List<HiveObjectPrivilege> hiveObjectPrivileges = this.listTablePrivileges(dbName, tableName, owner, principalType);

    Map<String, List<PrivilegeGrantInfo>> privGrants = Maps.newHashMap();
    List<PrivilegeGrantInfo> grants = Lists.newArrayList();
    for (HiveObjectPrivilege hiveObjectPrivilege : hiveObjectPrivileges) {
      grants.add(hiveObjectPrivilege.getGrantInfo());
    }

    privGrants.put(owner, grants);
    return privGrants;
  }

  /**
   * TODO.
   *
   * @param dbName TODO
   * @param tableName TODO
   * @param partitionName TODO
   * @return TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public synchronized Partition getPartition(String dbName, String tableName, String partitionName)
      throws HiveMetastoreException {

    try {
      connectIfNeeded();
      return client.getPartition(dbName, tableName, partitionName);
    } catch (NoSuchObjectException e) {
      return null;
    } catch (MetaException e) {
      // Brittle code - this was added to handle an issue with the Hive
      // Metstore. The MetaException is thrown when a table is
      // partitioned with one schema but the name follows a different one.
      // It's impossible to differentiate from that case and other
      // causes of the MetaException without something like this.
      if ("Invalid partition key & values".equals(e.getMessage())) {
        return null;
      } else {
        throw new HiveMetastoreException(e);
      }
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  /**
   * TODO.
   *
   * @param dbName TODO
   * @param tableName TODO
   * @param partition TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public synchronized void alterPartition(String dbName, String tableName, Partition partition)
      throws HiveMetastoreException {
    try {
      connectIfNeeded();
      client.alter_partition(dbName, tableName, partition);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  /**
   * TODO.
   *
   * @param dbName TODO
   * @param tableName TODO
   * @param table TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public synchronized void alterTable(String dbName, String tableName, Table table)
      throws HiveMetastoreException {
    try {
      connectIfNeeded();
      client.alter_table(dbName, tableName, table);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  /**
   * TODO.
   *
   * @param dbName TODO
   * @param tableName TODO
   * @return TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public boolean isPartitioned(String dbName, String tableName) throws HiveMetastoreException {
    Table table = getTable(dbName, tableName);
    return table != null && table.getPartitionKeys().size() > 0;
  }

  /**
   * TODO.
   *
   * @param dbName TODO
   * @param tableName TODO
   * @param partitionName TODO
   * @return TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public synchronized boolean existsPartition(String dbName, String tableName, String partitionName)
      throws HiveMetastoreException {
    return getPartition(dbName, tableName, partitionName) != null;
  }

  /**
   * TODO.
   *
   * @param dbName TODO
   * @param tableName TODO
   * @return TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public synchronized boolean existsTable(String dbName, String tableName)
      throws HiveMetastoreException {
    return getTable(dbName, tableName) != null;
  }

  @Override
  public void createTable(Table table) throws HiveMetastoreException {
    try {
      connectIfNeeded();
      client.createTable(table);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }


  /**
   * TODO.
   *
   * @param dbName TODO
   * @param tableName TODO
   * @param deleteData TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public synchronized void dropTable(String dbName, String tableName, boolean deleteData)
      throws HiveMetastoreException {
    try {
      connectIfNeeded();
      client.dropTable(dbName, tableName, deleteData, true);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  /**
   * TODO.
   *
   * @param dbName TODO
   * @param tableName TODO
   * @param partitionName TODO
   * @param deleteData TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public synchronized void dropPartition(String dbName, String tableName, String partitionName,
      boolean deleteData) throws HiveMetastoreException {
    try {
      connectIfNeeded();
      client.dropPartition(dbName, tableName, partitionName, deleteData);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  /**
   * TODO.
   *
   * @param partitionName TODO
   * @return TODO
   */
  public synchronized Map<String, String> partitionNameToMap(String partitionName)
      throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.partitionNameToSpec(partitionName);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public synchronized void createDatabase(Database db) throws HiveMetastoreException {
    try {
      connectIfNeeded();
      client.createDatabase(db);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public synchronized Database getDatabase(String dbName) throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.getDatabase(dbName);
    } catch (NoSuchObjectException e) {
      return null;
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public synchronized boolean existsDb(String dbName) throws HiveMetastoreException {
    return getDatabase(dbName) != null;
  }

  @Override
  public synchronized List<String> getPartitionNames(String dbName, String tableName)
      throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.listPartitionNames(dbName, tableName, (short) -1);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public synchronized List<String> getTables(String dbName, String tableName)
      throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.getTables(dbName, tableName);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public synchronized Partition exchangePartition(
      Map<String, String> partitionSpecs,
      String sourceDb, String sourceTable,
      String destDb,
      String destinationTableName)
          throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.exchange_partition(partitionSpecs, sourceDb, sourceTable, destDb,
          destinationTableName);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public void renamePartition(
      String db,
      String table,
      List<String> partitionValues,
      Partition partition)
      throws HiveMetastoreException {
    try {
      connectIfNeeded();
      client.renamePartition(db, table, partitionValues, partition);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  /**
   * TODO.
   *
   * @return TODO
   */
  public List<String> getAllDatabases() throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.getAllDatabases();
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  /**
   * TODO.
   *
   * @param dbName TODO
   * @return TODO
   */
  public List<String> getAllTables(String dbName) throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.getAllTables(dbName);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  public List<HiveObjectPrivilege> listTablePrivileges(String dbName, String tableName, String grantorName, PrincipalType principalType) throws TException {
    List<HiveObjectPrivilege> privileges = Lists.newArrayList();
    HiveObjectRef hiveObj = new HiveObjectRef(HiveObjectType.TABLE, dbName, tableName, null, null);
    privileges.addAll(client.list_privileges(grantorName, principalType, hiveObj));
    return privileges;
  }
}
