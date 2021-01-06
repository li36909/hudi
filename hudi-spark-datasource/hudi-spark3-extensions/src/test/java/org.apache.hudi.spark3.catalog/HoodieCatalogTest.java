package org.apache.hudi.spark3.catalog;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.spark3.sql.HoodieSparkSessionExtension;

import org.apache.spark.sql.SparkSession;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The HoodieCatalogTest.
 *
 * @since 2021/1/8
 */
public class HoodieCatalogTest {

  SparkSession sparkSession;

  @BeforeAll
  public void setUp() throws Exception {
    System.setProperty("test.tmp.dir", org.apache.spark.util.Utils$.MODULE$.createTempDir(System.getProperty("java.io.tmpdir"), "hudi").toURI().getPath());
    sparkSession = SparkSession.builder().appName("createTable").master("local[*]").config("spark.sql.catalog.spark_catalog", "org.apache.hudi.spark3.catalog.HoodieCatalog")
        //.config("spark.ui.enabled", false)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").config("spark.default.parallelism", "1").config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", false).withExtensions(new HoodieSparkSessionExtension()).enableHiveSupport().getOrCreate();
  }

  @AfterAll
  public void tearDown() throws Exception {
    sparkSession.stop();
  }

  @Test
  public void createBaseCOWTable() {
    sparkSession.sql("drop table if exists test1");
    sparkSession.sql("create table test1(a int, b string) " + "using org.apache.hudi.spark3.internal " + "options('hoodie.datasource.write.precombine.field'='a',"
        + "'hoodie.datasource.write.recordkey.field'='a')");
    sparkSession.sql("show tables").show();
  }

  @Test
  public void createPartitionCOWTable() {
    sparkSession.sql("drop table if exists test2");
    sparkSession.sql("create table test2(a int, b string) " + "using org.apache.hudi.spark3.internal partitioned by(b) " + "options('hoodie.datasource.write.precombine.field'='a',"
        + "'hoodie.datasource.write.recordkey.field'='a'," + "'hoodie.datasource.hive_sync.partition_extractor_class'='org.apache.hudi.hive.MultiPartKeysValueExtractor')");
    sparkSession.sql("show tables").show();
  }

  @Test
  public void createAsSelectCOWTable() {
    sparkSession.sql("drop table if exists test3");
    sparkSession.sql("create table test3 " + "using org.apache.hudi.spark3.internal partitioned by(b) " + "options('hoodie.datasource.write.precombine.field'='a',"
        + "'hoodie.datasource.write.recordkey.field'='a'," + "'hoodie.datasource.hive_sync.partition_extractor_class'='org.apache.hudi.hive.MultiPartKeysValueExtractor') "
        + "as select 1 as a,'a' as b");
    // sparkSession.sql("select * from test3").show();
  }

  @Test
  public void createMultiplePartitionCOWTable() {
    sparkSession.sql("drop table if exists test4");
    sparkSession.sql("create table test4 " + "using org.apache.hudi.spark3.internal partitioned by(year,month,day) " + "options('hoodie.datasource.write.precombine.field'='a',"
        + "'hoodie.datasource.write.recordkey.field'='a'," + "'hoodie.datasource.hive_sync.partition_extractor_class'='org.apache.hudi.hive.MultiPartKeysValueExtractor') "
        + "as select 'a' as a,'b' as b,'2021' as year,'1' as month,'12' as day");
  }

  @Test
  public void createMultiplePartitionMORTable() {
    sparkSession.sql("drop table if exists test5_rt");
    sparkSession.sql("drop table if exists test5_ro");
    sparkSession.sql("create table test5 " + "using org.apache.hudi.spark3.internal partitioned by(year,month,day) " + "options('hoodie.table.type'='MERGE_ON_READ',"
        + "'hoodie.datasource.write.precombine.field'='a'," + "'hoodie.datasource.write.recordkey.field'='a',"
        + "'hoodie.datasource.hive_sync.partition_extractor_class'='org.apache.hudi.hive.MultiPartKeysValueExtractor') " + "as select 'a1' as a,'b1' as b,'2021' as year,'1' as month,'13' as day");
  }

  @Test
  public void queryTable() {
    sparkSession.sql("select * from test5_rt where day=13").show(false);
  }

  @Test
  public void relationTest() {
    System.setProperty("test.tmp.dir", org.apache.spark.util.Utils$.MODULE$.createTempDir(System.getProperty("java.io.tmpdir"), "hudi").toURI().getPath());
    sparkSession = SparkSession.builder().appName("createTable").master("local[*]")
        //.config("spark.sql.catalog.spark_catalog", "org.apache.hudi.spark3.catalog.HoodieCatalog")
        .config("spark.ui.enabled", false).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").config("spark.default.parallelism", "1").config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", false)
        //.withExtensions(new HoodieSparkSessionExtension())
        //.enableHiveSupport()
        .getOrCreate();
    sparkSession.read().format("org.apache.hudi").option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL())
        .load("hdfs://hacluster/user/hive/warehouse/test5/*/*/*").show();
  }
}