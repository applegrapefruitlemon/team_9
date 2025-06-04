# 기상 예보파일 (날짜 기준으로 병합됨) 을 전체 범위에서 서울 - 경기 격자 좌표 범위만 필터링 후 저장

import os
import pandas as pd

# 서울·경기 격자 범위
x_min, x_max = 50, 66
y_min, y_max = 119, 135

# 루트 폴더
input_root = 'forecast_2022_2024'
output_root = 'filtered_forecast_2022_2024'
os.makedirs(output_root, exist_ok=True)

# 연도 폴더 순회
for year_folder in sorted(os.listdir(input_root)):
    year_path = os.path.join(input_root, year_folder)
    if not os.path.isdir(year_path):
        continue

    out_year_path = os.path.join(output_root, year_folder)
    os.makedirs(out_year_path, exist_ok=True)

    # 월별 폴더 순회
    for month_folder in sorted(os.listdir(year_path)):
        month_path = os.path.join(year_path, month_folder)
        if not os.path.isdir(month_path):
            continue

        for file_name in os.listdir(month_path):
            if not file_name.endswith('.txt'):
                continue

            file_path = os.path.join(month_path, file_name)
            print(f"📂 처리 중: {file_name}")

            try:
                # 1. 값 로드
                values = []
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line_values = line.strip().split(',')
                        for val in line_values:
                            val = val.strip()
                            if val:
                                try:
                                    values.append(float(val))
                                except ValueError:
                                    print(f"⚠️ 변환 실패 값: {val}")

                # 2. 좌표 생성
                x_range = range(149)
                y_range = range(253)
                coords = [(x, y) for x in x_range for y in y_range]

                if len(values) != len(coords):
                    print(f"❌ 값 개수 {len(values)} ≠ 좌표 개수 {len(coords)} → {file_name} 스킵")
                    continue

                # 3. DataFrame 생성
                df = pd.DataFrame(coords, columns=['nx', 'ny'])
                df['value'] = values

                # 4. 필터링
                filtered_df = df[(df['nx'] >= x_min) & (df['nx'] <= x_max) &
                                 (df['ny'] >= y_min) & (df['ny'] <= y_max)]

                # 5. 저장 (연도 폴더에만 저장)
                out_file_path = os.path.join(out_year_path, file_name.replace('.txt', '.csv'))
                filtered_df.to_csv(out_file_path, index=False)

                print(f"✅ 저장 완료: {out_file_path} | {len(filtered_df)}개 행")

            except Exception as e:
                print(f"❌ 오류 발생: {file_name} | {e}")
