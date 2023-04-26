# -*- coding: utf-8 -*-
### 모듈 임포트 ###
import asyncio
import json

import requests
import websockets
import json


# 웹소켓 접속키 발급
def get_approval(key, secret):
    # url = https://openapivts.koreainvestment.com:29443' # 모의투자계좌
    url = 'https://openapi.koreainvestment.com:9443'  # 실전투자계좌
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": key,
        "secretkey": secret
    }
    res = requests.post(f"{url}/oauth2/Approval", headers=headers, data=json.dumps(body))
    approval_key = res.json()["approval_key"]
    return approval_key


# 국내주식체결처리 출력라이브러리
def stockspurchase_domestic(data_cnt, data):
    print("============================================")
    # menulist = "유가증권단축종목코드|주식체결시간|주식현재가|전일대비부호|전일대비|전일대비율|가중평균주식가격|주식시가|주식최고가|주식최저가|매도호가1|매수호가1|체결거래량|누적거래량|누적거래대금|매도체결건수|매수체결건수|순매수체결건수|체결강도|총매도수량|총매수수량|체결구분|매수비율|전일거래량대비등락율|시가시간|시가대비구분|시가대비|최고가시간|고가대비구분|고가대비|최저가시간|저가대비구분|저가대비|영업일자|신장운영구분코드|거래정지여부|매도호가잔량|매수호가잔량|총매도호가잔량|총매수호가잔량|거래량회전율|전일동시간누적거래량|전일동시간누적거래량비율|시간구분코드|임의종료구분코드|정적VI발동기준가"
    menulist = "유가증권단축종목코드|주식체결시간|주식현재가"
    menustr = menulist.split('|')
    p_value = data.split('^')
    i = 0
    for cnt in range(data_cnt):  # 넘겨받은 체결데이터 개수만큼 print 한다
        print("### [%d / %d]" % (cnt + 1, data_cnt))
        for menu in menustr:
            print("%-13s[%s]" % (menu, p_value[i]))
            i += 1


async def connect():
    g_appkey = "PSI0as1BPniCODQ3Q21MG7AnNWIGYo2P1X6d"
    g_appsceret = "lseDWB1cThv2y+/mK89nRcMTvuY3qwXoTaijA+7X7qqzIVTzd9fFeCDQ1tlSW+T7d8A8RJ9VfA3LKMpsdBLrE2/Qv4/c+xBMcn9sb2IzT5cm4WYwXW5ZgRfBUfN3FWK+BOdgNnrd4fvJCUEx09fDYwo2Mmh0lK5ZfJtytHfMIS9xPkzt27o="
    g_approval_key = get_approval(g_appkey, g_appsceret)
    print("approval_key [%s]" % (g_approval_key))

    # url = 'ws://ops.koreainvestment.com:31000' # 모의투자계좌
    url = 'ws://ops.koreainvestment.com:21000'  # 실전투자계좌

    # 원하는 호출을 [tr_type, tr_id, tr_key] 순서대로 리스트 만들기
    code_list = [
        ['1', 'H0STCNT0', '005930'],
        ['1', 'H0STCNT0', '000270']
    ]

    senddata_list = []

    for tr_type, tr_id, tr_key in code_list:
        x = {
            "header": {
                "approval_key": g_approval_key,
                "custtype": "P",
                "tr_type": tr_type,
                "content-type": "utf-8"
            },
            "body": {
                "input": {
                    "tr_id": tr_id,
                    "tr_key": tr_key
                }
            }
        }
        x = json.dumps(x)
        senddata_list.append(x)

    while True:
        async with websockets.connect(url, ping_interval=30) as websocket:
            for senddata in senddata_list:
                await websocket.send(senddata)
                await asyncio.sleep(0)
                print(f"Input Command is :{senddata}")
            while True:
                try:
                    data = await websocket.recv()
                    await asyncio.sleep(0)
                    # print(f"Recev Command is :{data}")  # 정제되지 않은 Request / Response 출력

                    if data[0] == '0':
                        recvstr = data.split('|')  # 수신데이터가 실데이터 이전은 '|'로 나뉘어져있어 split
                        trid0 = recvstr[1]
                        if trid0 == "H0STCNT0":  # 주식체결 데이터 처리
                            print("#### 주식체결 ####")
                            data_cnt = int(recvstr[2])  # 체결데이터 개수
                            stockspurchase_domestic(data_cnt, recvstr[3])

                except websockets.ConnectionClosed:
                    continue


def main():
    # 비동기로 서버에 접속한다.
    asyncio.get_event_loop().run_until_complete(connect())
    asyncio.get_event_loop().close()
