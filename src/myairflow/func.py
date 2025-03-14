# myetl function
import pandas as pd
import pyarrow.parquet as pq
import os

def load_data_pq(dis):
    dir_path = "/home/lucas/data/"
    df = pd.read_csv(f"{dir_path}{dis}/data.csv")
    df.to_parquet(f"{dir_path}{dis}/data.parquet", engine='pyarrow')

def save_agg_csv(dis):
    dir_path = "/home/lucas/data/"
    df = pd.read_parquet(f"{dir_path}{dis}/data.parquet", engine='pyarrow')
    gdf = df.groupby(["name", "value"]).size().reset_index(name="count")
    gdf.to_csv(f"{dir_path}{dis}/agg.csv", index=False)

# CSV_PATH = "/home/lucas/data/data.csv"
# PARQUET_PATH = "/home/lucas/data/data.parquet"
# AGG_CSV_PATH = "/home/lucas/data/agg.csv"

# def load_data_pq(b):
   # if os.path.exists(CSV_PATH):
    #    df = pd.read_csv(CSV_PATH)
     #   df.to_parquet(PARQUET_PATH, engine="pyarrow")
      #  print(f"CSV → Parquet 변환 완료: {PARQUET_PATH}")
       # return True
   # else:
    #    print(f"오류: CSV 파일을 찾을 수 없습니다! ({CSV_PATH})")
     #   return False

# def save_agg_csv(a):
   # if os.path.exists(PARQUET_PATH):
     #   df = pd.read_parquet(PARQUET_PATH)

        # Group By(name) + count(value)
      #  agg_df = df.groupby("name")["value"].count().reset_index()
       # agg_df.rename(columns={"value": "count"}, inplace=True)

        # CSV 저장
        # agg_df.to_csv(AGG_CSV_PATH, index=False)
        # print(f"Parquet Group By → CSV 저장 완료: {AGG_CSV_PATH}")
        # return True
    # else:
      #  print(f"오류: Parquet 파일을 찾을 수 없습니다! ({PARQUET_PATH})")
       #  return False
