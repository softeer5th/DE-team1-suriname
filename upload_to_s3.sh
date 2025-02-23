#!/bin/bash

output_dir="./split_parquet"
s3_bucket="aws-seoul-suriname"
s3_prefix="data/community"

for file in $(ls $output_dir); do
    base_name=$(basename $file .parquet)
    
    start_time=${base_name:0:10}  # 시작 시간 (예: 2407010000)
    end_time=${base_name:11:10}   # 끝 시간 (예: 2407010600)
    
    # 날짜 포맷 변환 (YYMMDDHH -> YYYY-MM-DD-HH-00-00)
    start_time_fmt="20${start_time:0:2}-${start_time:2:2}-${start_time:4:2}-${start_time:6:2}-00-00"
    end_time_fmt="20${end_time:0:2}-${end_time:2:2}-${end_time:4:2}-${end_time:6:2}-00-00"

    # MacOS와 Linux에 맞춰 폴더 시간 변환
    if [[ "$OSTYPE" == "darwin"* ]]; then
        folder_time=$(date -j -v-1H -f "%Y-%m-%d-%H-%M-%S" "$end_time_fmt" +"%Y-%m-%d-%H-00-00")
    else
        folder_time=$(date -d "$end_time_fmt -1 hour" +"%Y-%m-%d-%H-00-00")
    fi

    s3_path="s3://$s3_bucket/$s3_prefix/${folder_time}_${end_time_fmt}/"
    
    echo "Uploading $file to $s3_path"
    aws s3 cp "$output_dir/$file" "$s3_path"
done