import pandas as pd
import numpy as np
from pyspark.sql import DataFrame
import shutil
import datetime as dt
import os
import glob
from airflow.models import Variable

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


def write_spark(spark, df, subdir, date):
    if date != None:
        file_path = DL_WRITE_DIR.format(subdir=subdir, date=str(date)[:10])
    else:
        SHORT_DIR = "/".join(DL_WRITE_DIR.split("/")[:-2]) + "/"
        file_path = SHORT_DIR.format(subdir=subdir)
    if isinstance(df, DataFrame) == False:
        df_sp = spark.createDataFrame(df)
    else:
        df_sp = df
    _ = (
        df_sp.write.format("orc")
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


def read_protect_parquet(path):
    try:
        return pd.read_parquet(path)
    except:
        return pd.DataFrame()


def read_many_parquet(dir: str) -> pd.DataFrame:
    """
    Read partitioned data in CSV format.
    """
    file_list = glob.glob(dir + "*.parquet")
    df = pd.concat([read_protect_parquet(x) for x in file_list], ignore_index=True)
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
            df[col] = pd.to_datetime(df[col]).apply(lambda r: r.date())
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
