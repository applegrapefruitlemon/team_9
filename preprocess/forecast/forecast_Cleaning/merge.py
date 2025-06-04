# 각 기상변수에 대한 각각의 파일을 날짜 기준으로 하나로 병합
# ( 20220102_TMP ... -> 20220102.csv)
import os
import pandas as pd
from collections import defaultdict

# ✅ 시작과 종료 기준 시간 (YYYYMMDDHH 형식)
start_time = "2023010102"
end_time   = "2024123123"

input_root = 'filtered_forecast_2022_2024'
output_root = 'merged_forecast_2022_2024'
os.makedirs(output_root, exist_ok=True)

for year_folder in sorted(os.listdir(input_root)):
    year_path = os.path.join(input_root, year_folder)
    if not os.path.isdir(year_path):
        continue

    out_year_path = os.path.join(output_root, year_folder)
    os.makedirs(out_year_path, exist_ok=True)

    file_groups = defaultdict(list)

    for file_name in os.listdir(year_path):
        if not file_name.endswith('.csv'):
            continue
        try:
            base_time = file_name.split('_')[0]  # 예: 2022010102
            if base_time < start_time or base_time > end_time:
                continue  # 범위 밖이면 무시
            file_groups[base_time].append(os.path.join(year_path, file_name))
        except Exception as e:
            print(f"❌ 파일명 파싱 실패: {file_name} | {e}")
            continue

    for base_time, paths in file_groups.items():
        merged_df = None
        for path in paths:
            var = os.path.basename(path).replace('.csv', '').split('_')[-1]
            df = pd.read_csv(path)

            if var not in df.columns:
                df = df.rename(columns={df.columns[-1]: var})

            if merged_df is None:
                merged_df = df
            else:
                merged_df = pd.merge(merged_df, df, on=['nx', 'ny'], how='outer')

        if merged_df is not None and not merged_df.empty:
            out_path = os.path.join(out_year_path, f"{base_time}.csv")
            merged_df.to_csv(out_path, index=False)
            print(f"✅ 저장 완료: {out_path}")
        else:
            print(f"⚠️ 병합 실패 또는 데이터 없음: {base_time}")
