import unittest
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from slowly_changing_dimension.lib.scd_component import get_open_and_closed, no_change_or_update, new_rows, deleted_rows
from pyspark.sql.functions import col, lit, to_timestamp
from datetime import datetime

class SCDTest(unittest.TestCase):

    ROW_JSON = {"app_id": "13ff8629-c1fc-e289-e81f-bc8c8968e9d6", "feature": 1}
    _spark = SparkSession.builder.appName("Spark Benchmarking").master("local[*]").getOrCreate()
    _sc = SparkContext.getOrCreate()
    current = _spark.read.json(_sc.parallelize([ROW_JSON]))
    history = (current
               .withColumn("start_date", to_timestamp(lit("2000-01-01 00:01:02"), "yyyy-MM-dd HH:mm:ss"))
               .withColumn("end_date", to_timestamp(lit("2999-12-31 00:00:00"), "yyyy-MM-dd HH:mm:ss"))
               .withColumn("open_reason", lit("new"))
               .withColumn("close_reason", lit(None))
               .withColumn("is_deleted", lit(0))
               )
    keys_list = ["app_id"]

    def test_no_end__or_close_date_in_history(self):
        self.assertRaises(Exception, get_open_and_closed(self.history.drop(col("end_date"))))

    def test_updated(self):
        open_rows, _ = get_open_and_closed(self.history)
        merged = no_change_or_update(history_open=open_rows.withColumn("feature", lit(5)),
                                     current=self.current,
                                     keys_list=self.keys_list)
        self.assertEqual(merged.count(), 2)

    def test_no_change(self):
        open_rows, _ = get_open_and_closed(self.history)
        merged = no_change_or_update(history_open=open_rows,
                                     current=self.current,
                                     keys_list=self.keys_list)
        self.assertEqual(merged.count(), 1)

    def test_new_row(self):
        open_rows, _ = get_open_and_closed(self.history)
        new_current = self.current.union(self.current.withColumn("app_id", lit("random_string_for_testing")))
        new_row_df = new_rows(history_open=open_rows,
                              current=new_current,
                              keys_list=self.keys_list)
        self.assertEqual(new_row_df.count(), 1)

    def test_delete_row(self):
        open_rows, _ = get_open_and_closed(self.history)
        # Intentional filter to have an empty dataframe
        empty_df = self.current.where("app_id=='1asfaf'")
        deleted_rows_df = deleted_rows(history_open=open_rows, current=empty_df, keys_list=self.keys_list)
        self.assertEqual(deleted_rows_df.count(), 2)

    def test_no_change_no_feature(self):
        open_rows, _ = get_open_and_closed(self.history.drop(col("feature")))
        merged = no_change_or_update(history_open=open_rows.drop(col("feature")),
                                     current=self.current.drop(col("feature")),
                                     keys_list=self.keys_list)
        self.assertEqual(merged.count(), 1)

    def test_new_row_no_feature(self):
        open_rows, _ = get_open_and_closed(self.history.drop(col("feature")))
        current_v2 = self.current.drop(col("feature")).unionByName(self.current
                                                                   .withColumn("app_id", lit("RANDOM_COLUMN")).drop(col("feature")))
        merged = new_rows(history_open=open_rows,
                          current=current_v2,
                          keys_list=self.keys_list)
        self.assertEqual(merged.count(), 1)

    def test_delete_no_feature(self):
        open_rows, _ = get_open_and_closed(self.history.drop(col("feature")))
        current_v2 = self.current.withColumn("app_id", lit("RANDOM_COLUMN")).drop(col("feature"))
        merged = deleted_rows(history_open=open_rows,
                              current=current_v2,
                              keys_list=self.keys_list)
        self.assertEqual(merged.count(), 2)

    def test_new_row_use_date_col(self):
        open_rows, _ = get_open_and_closed(self.history)
        open_rows = open_rows.where("app_id ='TotalGibberish'")
        current_v2 = (self.current
                      .withColumn("date_created", to_timestamp(lit("2010-01-01 00:01:02"), "yyyy-MM-dd HH:mm:ss")))
        merged = new_rows(history_open=open_rows,
                          current=current_v2,
                          keys_list=self.keys_list,
                          open_column="date_created")
        self.assertEqual(merged.select("start_date").collect()[0]["start_date"], datetime(2010, 1, 1, 0, 1, 2))


if __name__ == '__main__':
    unittest.main()
