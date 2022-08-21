from distutils.log import error
import pandas as pd
import numpy as np
from scipy import stats
from pyspark.sql import DataFrame
import shutil
import datetime as dt
import os
import glob
from airflow.models import Variable
import fmp.settings as fmp_s
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import pyspark.sql.types as T
import pyspark.sql.functions as F
from distutils.util import strtobool

sm_data_lake_dir = Variable.get("sm_data_lake_dir")
BUFFER_DIR = sm_data_lake_dir + "/buffer/{}/"
DL_WRITE_DIR = sm_data_lake_dir + "/{subdir}/{date}/"

## functions
def flip_sign(text):
    return "-" + text.strip("(").strip(")") if "(" in text else text


def percent(text):
    return float(text.strip("%")) / 100 if "%" in text else text


def int_extend(column):
    int_text = ["K", "M", "B", "T"]
    int_scale = [1000, 1000000, 1000000000, 1000000000]
    for t, s in zip(int_text, int_scale):
        column = column.apply(lambda row: flip_sign(str(row)))
        column = column.apply(
            lambda row: int(float(str(row).replace(t, "")) * s)
            if t in str(row)
            else row
        )
        column = column.apply(lambda row: percent(str(row)))
        column = column.apply(lambda row: np.nan if row == "-" else row)
    return column


def clear_buffer(subdir):
    try:
        print("clear buffer")
        dir_ = BUFFER_DIR.format(subdir)
        shutil.rmtree(dir_)
        os.mkdir(dir_)
    except Exception as e:
        print(e)
        pass  ##
    return True


def format_buffer(ds: dt.date, buffer_dir: str, yesterday: bool):
    """
    Clear buffer and make folder name the date
    to pass through the downstream spark applications.
    Convert ds type for airflow inputs.
    """
    ds = pd.to_datetime(ds).date()
    if strbool(yesterday):
        ds = ds - dt.timedelta(1)
    clear_buffer(buffer_dir.split("/data/buffer")[1])
    os.makedirs(buffer_dir + f"/{ds}")
    return True


def write_spark(spark, df, subdir, date, file_type="orc"):
    if date != None:
        file_path = DL_WRITE_DIR.format(subdir=subdir, date=str(date)[:10])
    else:
        SHORT_DIR = "/".join(DL_WRITE_DIR.split("/")[:-2]) + "/"
        file_path = SHORT_DIR.format(subdir=subdir)
    if isinstance(df, DataFrame) == False:
        df_sp = spark.createDataFrame(df)
    else:
        df_sp = df
    (
        df_sp.write.format(file_type)
        .mode("overwrite")
        .option("compression", "snappy")
        .save(file_path)
    )
    return True


def is_between(time, time_range):
    if time_range[1] < time_range[0]:
        return time >= time_range[0] or time <= time_range[1]
    return time_range[0] <= time <= time_range[1]


def read_protect(path):
    try:
        return pd.read_csv(path)
    except:
        return pd.DataFrame()


def read_many_csv(dir: str) -> pd.DataFrame:
    """
    Read partitioned data in CSV format.
    """
    file_list = glob.glob(dir + "*.csv")
    df = pd.concat([read_protect(x) for x in file_list], ignore_index=True)
    return df


def read_protect_parquet(path: str, params: dict = None):
    """
    Read a parquet file and slice, if needed.
    This is used to ready many files in python.
    """
    if os.path.isfile(path) | os.path.isdir(path):
        df = pd.read_parquet(path)
        if len(df) == 0:
            return pd.DataFrame()
        if params != None:
            if "evaluation" in params.keys():
                if params["evaluation"] == "equal":
                    df = df.loc[df[params["column"]] == params["slice"]]
                elif params["evaluation"] == "gt":
                    df = df.loc[df[params["column"]] > params["slice"]]
                elif params["evaluation"] == "lt":
                    df = df.loc[df[params["column"]] < params["slice"]]
                elif params["evaluation"] == "gt_e":
                    df = df.loc[df[params["column"]] >= params["slice"]]
                elif params["evaluation"] == "lt_e":
                    df = df.loc[df[params["column"]] <= params["slice"]]
                elif params["evaluation"] == "not_null":
                    if "columns" in params.keys():
                        for col in params["columns"]:
                            df = df.loc[df[col].notnull()]
                    else:
                        df = df.loc[df[params["column"]].notnull()]
                else:
                    raise ValueError("Incorrect evaluation method.")
            if "column_slice" in params.keys():
                df = df[list(params["column_slice"])]
        return df
    else:
        return pd.DataFrame()


def read_many_dir_parquet(
    dir: str, subdir_list: list = [], params: dict = None
) -> pd.DataFrame:
    """
    Read all parquet files in a list of sub directories.
    """
    if dir[-1] != "/":
        dir = dir + "/"
    return pd.concat(
        [read_protect_parquet(dir + x, params) for x in subdir_list], ignore_index=True
    )


def read_many_parquet(dir: str, params: dict = None) -> pd.DataFrame:
    """
    Read partitioned data in CSV format.
    """
    if dir[-1] != "/":
        dir = dir + "/"
    file_list = glob.glob(dir + "*.parquet")
    df = pd.concat(
        [read_protect_parquet(x, params) for x in file_list], ignore_index=True
    )
    return df


def distribute_read_many_parquet(ds: dt.date, path: str, params: dict = None):
    """
    Read all tickers for a given day.
    This will be used to turn the ticker files
    to a daily stream.
    """
    to_collect_df = pd.read_parquet(fmp_s.to_collect + f"/{ds}.parquet")
    collection_list = to_collect_df["symbol"].values.tolist()
    distribution_list = [f"{path}/{ticker}.parquet" for ticker in collection_list]
    spark = SparkSession.builder.appName("read-files").getOrCreate()
    sc = spark.sparkContext
    dfs = (
        sc.parallelize(distribution_list)
        .map(lambda r: read_protect_parquet(r, params))
        .collect()
    )
    sc.stop()
    spark.stop()
    return pd.concat(dfs, ignore_index=True)


def distribute_read_orc(path: str, date_cols: list = []):
    """
    Read directory using orc.
    This is used for the Mary API.
    """
    if type(date_cols) != list:
        raise TypeError("date_col must be a list")
    if path[-1] == "/":
        path = path[:-1]
    spark = SparkSession.builder.appName("read-api").getOrCreate()
    df = spark.read.format("orc").option("path", path + "/*").load().toPandas()
    spark.stop()
    if len(date_cols) > 0:
        for col in date_cols:
            df[col] = pd.to_datetime(df[col]).apply(lambda r: r.date())
    return df


def rename_file(path: str, fn: str) -> bool:
    """
    Rename files written from spark to represent
    the date of the file.
    This function only works to rename a single
    CSV. If the file is not a CSV or if there
    is more than one file, it will break.

    Inputs:
        - Path where the file lives
        - fn is the new name of the file
    """
    file_list = glob.glob(path + "*.csv")
    if len(file_list) == 0:
        raise ValueError("No csv found")
    elif len(file_list) > 1:
        raise ValueError("Too many files found")

    ## rename
    _ = os.rename(file_list[0], path + str(fn) + ".csv")
    return True


def move_files(data_loc: str, date: dt.date, days: int, buffer_loc: str) -> bool:
    """
    Move data from data-lake to buffer.
    This is very useful for large data processing.
    Since I know the dates I want to analyze,
    I can save exense by only loading them.
    Since spark filters through all partitioned data
    it can take a while. This only lets spark
    read the data it needs.

    Inputs:
        - Location of the data to move
        - Date is the last day of the analysis
        - Days is the lookback window
        - Location to move the data
    """
    ## assign end date
    date = pd.to_datetime(date).date()
    date_min = date - dt.timedelta(days=days)
    try:
        ## clear subset buffer
        clear_buffer(buffer_loc.split("data/buffer/")[1])
    except:
        ## not sure why this is sometimes dumb
        pass
    # Subset from data lake
    date_list = [x.date() for x in pd.date_range(date_min, date)]
    dl_loc_tmp = data_loc + "/{}"
    buffer_temp = buffer_loc + "/{}"
    for d in date_list:
        ## set location vars
        dl_location = dl_loc_tmp.format(d)
        b_loc = buffer_temp.format(d)
        ## migrate daily data
        try:
            _ = shutil.copytree(dl_location, b_loc)
        except FileNotFoundError as e:
            e  ## remove print
            pass
    return True


def skewed_simga(compare_200):
    ul = [x * 100 for x in compare_200 if x > 0]
    ll = [x * 100 for x in compare_200 if x <= 0]

    ul_sigma = np.array(ul).std() / 1000
    ll_sigma = np.array(ll).std() / 1000
    ul_mean = np.array(ul).mean() / 1000
    ll_mean = np.array(ll).mean() / 1000

    return [ul_sigma, ll_sigma, ul_mean, ll_mean]


def small_sigma(compare_200):
    return (compare_200 * 100).std() / 1000


def format_data(df: pd.DataFrame, types: dict) -> pd.DataFrame:
    """
    Format dataframe with correct types.
    This is the step before writing to parquet.
    """
    for col in df.columns:
        if types[col] == dt.date:
            df[col] = pd.to_datetime(df[col], errors="coerce").apply(lambda r: r.date())
        elif types[col] == dt.datetime:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif types[col] == bool:
            df[col] = (
                df[col]
                .apply(
                    lambda r: str(r).replace("true", "True").replace("false", "False")
                )
                .astype(types[col])
            )
        else:
            df[col] = df[col].astype(types[col])
    return df


def prep_parallelize(df: pd.DataFrame, agg_col: str) -> list:
    """
    Subset dataframe by aggregate column.
    Convert to list to pass to spark.
    """
    agg_list = df[agg_col].unique()
    df_gb = df.groupby(agg_col)
    return [(x, df_gb.get_group(x)) for x in agg_list]


def migrate(path: str, fn: str, extention: str, data: pd.DataFrame) -> bool:
    """
    Append newly collected data from buffer to data-lake
    location. If the data is collected for the first time,
    save as is. If data exists in the data-lake, append
    and deduplicate.

    Clean periods and forward slashes from file name.
    Two underscores is reserved for periods.
    Three underscores is reserved for forward slashes.

    Inputs:
        - path: Absolue path to data location.
        - fn: File name
        - extention: File tupe, e.g., parquet, cvs, etc.
        - data: Data to save
    """
    read_method = {"parquet": pd.read_parquet, "csv": pd.read_csv}
    fn_clean = fn.replace(".", "__").replace("/", "___")
    save_f = f"{path}/{fn_clean}.{extention}"
    if os.path.isfile(save_f):
        data = read_method[extention](save_f).append(data).drop_duplicates()
    if extention == "parquet":
        data.to_parquet(save_f, index=False)
    elif extention == "csv":
        data.to_csv(save_f, index=False)
    return True


def get_to_collect(
    buffer_dir: str,
):
    """
    Find list of tickers to distribute.
    Use the buffer directory to find the
    date to collect, then use the date
    to find the to-collect dataset.
    """
    ds = pd.to_datetime([x for x in os.walk(buffer_dir + "/")][0][1][0]).date()
    ## find date or max date
    try:
        return pd.read_parquet(fmp_s.to_collect + f"/{ds}.parquet")["symbol"].tolist()
    except FileNotFoundError as e:
        new_ds = max(
            [
                d
                for d in [
                    pd.to_datetime(x.split(".")[0]).date()
                    for x in [x for x in os.walk(fmp_s.to_collect)][0][2]
                ]
                if d <= ds
            ]
        )
        return pd.read_parquet(fmp_s.to_collect + f"/{new_ds}.parquet")[
            "symbol"
        ].tolist()


def make_input(key: str, value: str, kwarg_dict: dict):
    new_dict = kwarg_dict.copy()
    new_dict[key] = value
    return new_dict


def find_max_date(path: str, date_ceiling: dt.date = dt.datetime.now()):
    """Find max date from datalake source."""
    date_list = [
        pd.to_datetime(x.split("/")[-1].split(".")[0]) for x in glob.glob(path + "/*")
    ]
    return max([x.date() for x in date_list if x < date_ceiling])


def get_high_volume(ds: dt.date, v_threshold: int = 500000, p_threshold: int = 10):
    """Get tickers with high volume for a given date."""
    max_date = find_max_date(fmp_s.avg_price, ds)
    spark = SparkSession.builder.appName(f"read-volume-data").getOrCreate()
    df = (
        spark.read.format("parquet")
        .option("path", fmp_s.avg_price + f"/{max_date}.parquet")
        .load()
        .filter(
            (F.col("avg_volume_10") >= v_threshold)
            & (F.col("avg_price_5") >= p_threshold)
        )
        .select("symbol")
        .toPandas()
    )
    spark.stop()
    return df["symbol"].tolist()


def rolling_weighted_avg(r: list, weights: list):
    if len(r) == len(weights):
        return np.average(r, weights=weights)
    else:
        return np.nan


def rolling_regression(y, window):
    rolling_y = [x for x in pd.Series(y).rolling(window)]
    slope_list = []
    for y in rolling_y:
        if len(y) > 1:
            x = range(len(y))
            slope_list.append(stats.linregress(x, y).slope)
        else:
            slope_list.append(np.nan)
    return pd.Series(slope_list)


def rolling_scale(r, window):
    if len(r) == window:
        ## Stretch z-scores to be between -3 and 3
        f_max = r.max()
        f_min = r.min()
        f_bar = (f_max + f_min) / 2
        A = 2 / (f_max - f_min)
        return list(map(lambda x: round(A * (x - f_bar), 4), r))[-1]
    else:
        return np.nan


def strbool(str_: str):
    """
    Convert string to boolean.
    This is helpful for airflow variables.
    """
    if type(str_) == str:
        str_ = strtobool(str_)
    return str_
