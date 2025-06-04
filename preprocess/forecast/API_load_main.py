# 기상청 예보데이터의 각 호출 변수에 대하여 데이터를 호출하고 해당 파일을 저장한다.
# 실패할 경우 error_code.csv로 저장 후 따로 개별 재호출 (data_load_each.py)

import requests
import os
import time
import csv
from datetime import datetime, timedelta

# 설정
auth_key = '' #' ' 안에 인증키 입력
variables = ['TMP', 'WSD', 'SKY', 'PTY', 'POP', 'PCP', 'REH']
base_url = 'https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-dfs_shrt_grd'
root_dir = 'forecast_2022_2024'
log_file = os.path.join(root_dir, 'error_log.csv')
valid_hours = {2, 5, 8, 11, 14, 17, 20, 23}

# 요청 기간 설정
start_date = datetime(2022, 8, 19, 23)
end_date = datetime(2024, 12, 31, 23)

# 로그파일 헤더 생성
if not os.path.exists(log_file):
    with open(log_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['tmfc', 'variable', 'error'])

def log_failure(tmfc, var, error):
    with open(log_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([tmfc, var, error])

def is_dummy(content):
    return '-99' in content and all(val.strip() == '-99.0' for val in content.replace(',', ' ').split() if is_float(val))

def is_float(x):
    try:
        float(x)
        return True
    except:
        return False

def download_with_retry(url, params, tmfc, var, max_retries=3):
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                return response.text
            else:
                error_msg = f"상태코드 {response.status_code}"
                print(f"{error_msg} (시도 {attempt})")
        except Exception as e:
            error_msg = str(e)
            print(f"요청 오류 (시도 {attempt}): {error_msg}")
        time.sleep(1)
    log_failure(tmfc, var, error_msg)
    return None

prev_date = None
current_time = start_date

start_exec_time = datetime.now()
print(f"🚀 수집 시작: {start_exec_time.strftime('%Y-%m-%d %H:%M:%S')}")

while current_time <= end_date:
    if current_time.hour in valid_hours:
        tmfc = current_time.strftime('%Y%m%d%H')
        yyyy_mm = current_time.strftime('%Y_%m')
        sub_dir = os.path.join(root_dir, yyyy_mm)
        os.makedirs(sub_dir, exist_ok=True)

        for var in variables:
            filename = f"{tmfc}_{var}.txt"
            file_path = os.path.join(sub_dir, filename)

            need_download = True
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    content_check = f.read()
                if not is_dummy(content_check):
                    print(f"이미 있음 (정상): {filename}")
                    need_download = False
                else:
                    print(f"🛠️ 더미 파일 감지, 재다운로드: {filename}")

            if not need_download:
                continue

            params = {
                'tmfc': tmfc,
                'vars': var,
                'authKey': auth_key
            }

            content = download_with_retry(base_url, params, tmfc, var)
            if content and not is_dummy(content):
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"✅ 저장 완료: {yyyy_mm}/{filename}")
            else:
                print(f"⚠️ 더미 응답 혹은 실패: {yyyy_mm}/{filename}")
                log_failure(tmfc, var, "dummy or empty content")

        time.sleep(1.5)

    if prev_date is None or prev_date != current_time.date():
        if prev_date is not None:
            print(f"{prev_date} 완료 → 30초 delay")
            time.sleep(30)
        prev_date = current_time.date()

    current_time += timedelta(hours=1)

end_exec_time = datetime.now()
print(f"✅ 수집 종료: {end_exec_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"⏱️ 총 소요 시간: {str(end_exec_time - start_exec_time)}")
