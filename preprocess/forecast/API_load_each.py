import requests
import os

# 기본 설정
auth_key = ''  # ← 여기에 본인의 인증키를 입력하세요
variables = ['TMP', 'WSD', 'SKY', 'PTY', 'POP', 'PCP', 'REH']
base_url = 'https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-dfs_shrt_grd'

# 조회 시간 (2022년 1월 1일 02시)
tmfc = '2023022820'

# 저장 디렉토리 생성
save_dir = 'forecast_add'
os.makedirs(save_dir, exist_ok=True)

# 요청 및 저장
for var in variables:
    params = {
        'tmfc': tmfc,
        'authKey': auth_key,
        'vars': var
    }

    try:
        response = requests.get(base_url, params=params, timeout=10)
        if response.status_code == 200:
            filename = f"{tmfc}_{var}.txt"
            filepath = os.path.join(save_dir, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(response.text)
            print(f"✅ 저장 완료: {filename}")
        else:
            print(f"❌ 실패: {tmfc}_{var} / 상태코드 {response.status_code}")
    except Exception as e:
        print(f"❌ 예외 발생: {tmfc}_{var} / {e}")
