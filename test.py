import pandas as pd
import glob
import os
from datetime import datetime, timedelta

# 원본 Parquet 파일 경로 설정
input_files = [
    "femco.parquet"
]

# 모든 Parquet 파일을 DataFrame으로 읽기
df_list = [pd.read_parquet(file) for file in input_files]
df = pd.concat(df_list, ignore_index=True)

# post_time 컬럼을 datetime 형식으로 변환
df["post_time"] = pd.to_datetime(df["post_time"])

# 날짜별로 1시간 단위로 분할하여 저장
output_dir = "./split_parquet/"  # 저장할 디렉토리
os.makedirs(output_dir, exist_ok=True)

date_min = df["post_time"].dt.floor("1H")  # 1시간 단위로 시작 시간 설정

for period_start in date_min.unique():
    period_end = period_start + timedelta(hours=6)  # 6시간 간격 설정
    mask = (df["post_time"] >= period_start) & (df["post_time"] < period_end)
    group = df[mask]
    
    if not group.empty:
        period_start_str = period_start.strftime("%y%m%d%H%M")
        period_end_str = period_end.strftime("%y%m%d%H%M")
        output_file = os.path.join(output_dir, f"{period_start_str}_{period_end_str}_femco.parquet")
        group.to_parquet(output_file, index=False)
        print(f"Saved: {output_file}")