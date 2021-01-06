package org.apache.hudi.spark3.sql

import java.io.File

import org.apache.hudi.DataSourceReadOptions.{QUERY_TYPE_OPT_KEY, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, QUERY_TYPE_SNAPSHOT_OPT_VAL}
import org.apache.hudi.keygen.constant.KeyGeneratorOptions

import scala.collection.JavaConverters._
import org.apache.hudi.{DataSourceUtils, DataSourceWriteOptions, DefaultSource}
import org.apache.hudi.spark3.internal.HoodieDataSourceInternalTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * The HoodieAnalysis.
 *
 * @since 2021/1/14
 */
class HoodieAnalysis(sparkSession: SparkSession, conf: SQLConf) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case HoodieV1Relation(relation) =>
      relation
  }
}

object HoodieV1Relation {
  def unapply(plan: LogicalPlan): Option[LogicalRelation] = plan match {
    case dsv2@DataSourceV2Relation(table: HoodieDataSourceInternalTable, _, _, _, options) =>
      fromV2Relation(dsv2, table, options)
    case _ => None
  }

  def fromV2Relation(dsv2: DataSourceV2Relation,
                     table: HoodieDataSourceInternalTable,
                     options: CaseInsensitiveStringMap) = {
    val datasourceV1 = new DefaultSource
    var properties = table.properties().asScala.toMap ++ options.asScala.toMap
    val partitionPath = table.properties.asScala.getOrElse(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "")
      .split(",").map(_ => "*").mkString(File.separator)

    val tableName = properties.get(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY).getOrElse(table.name())
    if (tableName.endsWith("_rt")) {
      properties = properties ++ Map[String, String](QUERY_TYPE_OPT_KEY -> QUERY_TYPE_SNAPSHOT_OPT_VAL)
    } else if (tableName.endsWith("_ro")) {
      properties = properties ++ Map[String, String](QUERY_TYPE_OPT_KEY -> QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
    }
    properties = properties ++ Map("path" -> (properties("path") + File.separator + partitionPath))
    val relation = datasourceV1.createRelation(SparkSession.active.sqlContext, properties, table.schema)
    Some(LogicalRelation(relation, dsv2.output, None, false))
  }


}
