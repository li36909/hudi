package org.apache.hudi.spark3.sql

import org.apache.spark.sql.SparkSessionExtensions

/**
 * The HoodieSparkSessionExtension.
 *
 * @since 2021/1/14
 */
class HoodieSparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectResolutionRule { session =>
      new HoodieAnalysis(session, session.sessionState.conf)
    }
  }
}
