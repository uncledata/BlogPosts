from typing import List, Union
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, lit, hash, array, explode

SCD_COLS = ["start_date", "end_date", "is_deleted", "open_reason", "close_reason"]
EOW_DATE_STR = "2999-12-31 00:00:00"

def column_renamer(df: DataFrame, alias: str, keys_list: List, add_suffix: int = 1) -> DataFrame:
    """ Not applicable to keys_list or SCD_COLS.
    
    adding/removing suffix f"_{alias}" to columns of dataframe DF. Created in order to avoid mismatches on same columns exising in 
    both history + current DFs. 

    Args:
        df (DataFrame): dataframe to perform renaming on
        alias (str): alias to add as suffix
        keys_list (List): list of keys which are used for joins
        add_suffix (int, optional): 0 - remove suffix, 1- add suffix. Defaults to 1.

    Returns:
        DataFrame: dataframe with performed adding/removing of suffix
    """
    if add_suffix == 1:
        for column in set(df.columns)-set(SCD_COLS)-set(keys_list):
            df = df.withColumnRenamed(column, column+"_"+alias)
    else:
        for column in [cols for cols in df.columns if f"_{alias}" in cols]:
            df = df.withColumnRenamed(column, column.replace("_"+alias, ""))
    return df


def get_values_hash(dataframe: DataFrame, keys_list: List, ignored_columns: List = []) -> DataFrame:
    """ adding a hash column for faster check for updated values. Exluding SCD_COLS and keys_list columns. If there are
    only key and SCD columns will use 1 as hash value

    Args:
        dataframe (DataFrame): dataframe to add "hash" column to
        keys_list (List): keys_list which will be used for joins
        ignored_columns (List): columns which will not be included in hash apart from SCD_COLS and keys_list

    Returns:
        DataFrame: Dataframe with added column "hash"
    """
    cols = list(set(dataframe.columns) - set(keys_list)-set(SCD_COLS)-set(ignored_columns))
    columns = [col(column) for column in cols]
    if columns:
        return dataframe.withColumn("hash", hash(*columns))
    else:
        return dataframe.withColumn("hash", hash(lit(1)))


def get_open_and_closed(history: DataFrame) -> Union[DataFrame, DataFrame]:
    """ Splitting historical/end result table to Old closed rows and currently open ones. 
    Also validating if end_date/close_date is present in the dataframe. If not - throwing an exception

    Args:
        history (DataFrame): SCD/end table

    Returns:
        Union[DataFrame, DataFrame]: [open rows DF, closed rows DF]
    """
    col_for_close = "end_date"
    if "close_date" in history.columns:
        col_for_close = "end_date"
    elif "end_date" not in history.columns:
        Exception("No end_date/close_date in the history DF")

    return history.where(col(col_for_close) >= to_timestamp(lit("2500-12-31"))), \
           history.where(col(col_for_close) < to_timestamp(lit("2500-12-31")))

def no_change_or_update(history_open: DataFrame,
                        current: DataFrame,
                        keys_list: List, close_date: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")) -> DataFrame:
    """ handling of updated or not changed rows.

    Args:
        history_open (DataFrame): open SCD rows
        current (DataFrame): current status
        keys_list (List): keys list used for joins
        close_date (str, optional): close_date to use if there is a specific you prefer (maybe ds/next_ds if it's from airflow).
        Defaults to datetime.now().strftime("%Y-%m-%d %H:%M:%S").

    Returns:
        DataFrame: Spark DF with handled updated/not changed rows part only
    """

    history_open_hash = column_renamer(get_values_hash(history_open, keys_list), alias="history", keys_list=keys_list)
    current_hash = column_renamer(get_values_hash(current, keys_list), alias="current", keys_list=keys_list)

    not_changed = column_renamer(history_open_hash
                                 .join(other=current_hash, on=keys_list, how="inner")
                                 .where(history_open_hash["hash_history"] == current_hash["hash_current"])
                                 .drop(*["hash_history", "hash_current"])
                                 .drop(*[column for column in current_hash.columns if "_current" in column])
                                 , alias="history", keys_list=keys_list, add_suffix=0).select(history_open.columns)

    changed = (history_open_hash
               .join(other=current_hash, on=keys_list, how="inner")
               .where(history_open_hash["hash_history"] != current_hash["hash_current"])
               .withColumn("hist_flag", array([lit(x) for x in [0, 1]]))
               .withColumn("hist_flag", explode(col("hist_flag")))
               )
    upd_closed = (column_renamer((changed.where("hist_flag=1")
                                  .withColumn("end_date", to_timestamp(lit(close_date), "yyyy-MM-dd HH:mm:ss"))
                                 .drop(*["hash_history", "hash_current", "hist_flag"])
                                 .drop(*[column for column in changed.columns if "_current" in column])
                                 ), alias="history", keys_list=keys_list, add_suffix=0).withColumn("close_reason",
                                                                                                   lit("changed_value"))
                  .select(history_open.columns))

    upd_new = (column_renamer((changed.where("hist_flag=0")
                               .withColumn("start_date", to_timestamp(lit(close_date), "yyyy-MM-dd HH:mm:ss"))
                               .withColumn("end_date", to_timestamp(lit(EOW_DATE_STR), "yyyy-MM-dd HH:mm:ss"))
                               .drop(*["hash_history", "hash_current", "hist_flag"])
                               .drop(*[column for column in changed.columns if "_history" in column])
                               ), alias="current", keys_list=keys_list, add_suffix=0)
               .withColumn("close_reason", lit(None))
               .withColumn("open_reason", lit("changed_value"))
               .select(history_open.columns))

    return (not_changed
            .unionByName(upd_new)
            .unionByName(upd_closed))


def new_rows(history_open: DataFrame,
             current: DataFrame,
             keys_list: List,
             open_date: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
             open_column: str = None) ->DataFrame:
    """Handling new rows insertion. If open column is supplied that column value will be used
    to fill start_date. open_date WILL BE IGNORED

    Args:
        history_open (DataFrame): SCD open rows DF
        current (DataFrame): current state
        keys_list (List): keys list for joins
        open_date (str, optional): custom open date if needed (i.e. from airflow for backfill).
            Defaults to datetime.now().strftime("%Y-%m-%d %H:%M:%S").
        open_column (str, optional): column name which is going to be used as start_date in SCD.
            By default no column will be used like this.

    Returns:
        DataFrame: Only new rows part is returned
    """
    new = current.join(other=history_open, on=keys_list, how="left_anti")
    new = (new.withColumn("end_date", to_timestamp(lit(EOW_DATE_STR), "yyyy-MM-dd HH:mm:ss"))
              .withColumn("open_reason", lit("new"))
              .withColumn("close_reason", lit(None))
              .withColumn("is_deleted", lit(0)))
    if open_column:
        return new.withColumn("start_date", col(open_column))
    else:
        return new.withColumn("start_date", to_timestamp(lit(open_date)))


def deleted_rows(history_open: DataFrame,
                 current: DataFrame,
                 keys_list: List,
                 close_date: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")) -> DataFrame:
    """Handling of deleted rows - closing open rows and opening ones with is_deleted flag

    Args:
        history_open (DataFrame): SCD open rows
        current (DataFrame): current state DF
        keys_list (List): keys list for joins
        close_date (str, optional): Custom close_date can be passed (i.e. from Airflow). Defaults to datetime.now().strftime("%Y-%m-%d %H:%M:%S").

    Returns:
        DataFrame: DF with deleted rows changes
    """

    deleted = history_open.join(other=current, on=keys_list, how="left_anti")
    closed_rows = (deleted
                   .withColumn("end_date", to_timestamp(lit(close_date), "yyyy-MM-dd HH:mm:ss"))
                   .withColumn("close_reason", lit("deleted")))
    open_close_row = (deleted
                      .withColumn("start_date", to_timestamp(lit(close_date), "yyyy-MM-dd HH:mm:ss"))
                      .withColumn("end_date", to_timestamp(lit(EOW_DATE_STR), "yyyy-MM-dd HH:mm:ss"))
                      .withColumn("open_reason", lit("deleted"))
                      .withColumn("close_reason", lit(None))
                      .withColumn("is_deleted", lit(1)))

    return closed_rows.unionByName(open_close_row)


def scd(history: DataFrame,
        current: DataFrame,
        keys_list: List,
        custom_close_date: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")) -> DataFrame:
    history_open, history_closed = get_open_and_closed(history)
    upd_not_changed = no_change_or_update(history_open=history_open,
                                          current=current,
                                          keys_list=keys_list,
                                          close_date=custom_close_date)

    new = new_rows(history_open=history_open,
                   current=current,
                   keys_list=keys_list,
                   open_date=custom_close_date)

    deleted = deleted_rows(history_open=history_open,
                           current=current,
                           keys_list=keys_list,
                           close_date=custom_close_date)

    return (history_closed
            .unionByName(upd_not_changed)
            .unionByName(new)
            .unionByName(deleted))
