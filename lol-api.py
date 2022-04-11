import requests
import json
import pandas as pd
from pandas import json_normalize
import time

token = '****'
now_version = '12.6.'

#특정 유저의 닉네임으로 puuid 조회
def get_puuid_by_id(user_id):
    url = 'https://asia.api.riotgames.com/riot/account/v1/accounts/by-riot-id/'
    tag = 'KR'
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.118 Whale/2.11.126.23 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://developer.riotgames.com",
        "X-Riot-Token": token
    }

    res = requests.get(url+user_id+'/'+tag, headers = header)
    if res.status_code != 200:
        print(res.text)
        print('get puuid by id', user_id)
        print("ERROR OCCURED", res.status_code)
    puuid = json.loads(res.text)['puuid']
    return (puuid, res.status_code)

#유저 아이디로 상세정보 받아옴
def get_detail_user_from_id(user_id):
    url = 'https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/' + user_id
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.118 Whale/2.11.126.23 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://developer.riotgames.com",
        "X-Riot-Token": token
    }
    res = requests.get(url, headers = header)
    if res.status_code != 200:
        print(res.text)
        print('get detail user from id', user_id)
        print("ERROR OCCURED", res.status_code)
    user_detail = json.loads(res.text)
    return (user_detail, res.status_code)

#encrypted id로 더 자세한 유저 정보 받아옴
def get_more_detail_user_from_enid(enid):
    url = 'https://kr.api.riotgames.com/lol/league/v4/entries/by-summoner/' + enid
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.118 Whale/2.11.126.23 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://developer.riotgames.com",
        "X-Riot-Token": token
    }
    res = requests.get(url, headers = header)
    if res.status_code != 200:
        print(res.text)
        print('get more detail user from enid', enid)
        print("ERROR OCCURED", res.status_code)
    user_more_detail = json.loads(res.text)
    return (user_more_detail, res.status_code)

#챌,그마,마스터 유저받아옴
def get_cgmm_user(tier):
    url = 'https://kr.api.riotgames.com/lol/league/v4/' + tier + 'leagues/by-queue/RANKED_SOLO_5x5'
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.118 Whale/2.11.126.23 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://developer.riotgames.com",
        "X-Riot-Token": token
    }

    res = requests.get(url, headers=header)
    if res.status_code != 200:
        print(res.text)
        print('get cgmm tier', tier)
        print("ERROR OCCURED", res.status_code)
    user_list = json.loads(res.text)
    return (user_list, res.status_code)

#puuid count로 특정 유저의 게임 리스트 받아옴
def get_matches_from_puuid(puuid, count):
    url = 'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/'+puuid+'/ids?start=0&count=' + str(count)
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.118 Whale/2.11.126.23 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://developer.riotgames.com",
        "X-Riot-Token": token
    }

    res = requests.get(url, headers=header)
    if res.status_code != 200:
        print(res.text)
        print('get matches from puuid', puuid, count)
        print("ERROR OCCURED", res.status_code)
    game_list = json.loads(res.text)
    return (game_list, res.status_code)


#게임 종합 정보 정도, 킬이랑 데미지, 아이템산거 등등
def get_match_detail_from_game_id(game_id):
    url = 'https://asia.api.riotgames.com/lol/match/v5/matches/' + game_id
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.118 Whale/2.11.126.23 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://developer.riotgames.com",
        "X-Riot-Token": token
    }

    res = requests.get(url, headers=header)
    if res.status_code != 200:
        print(res.text)
        print('get match detail from game id', game_id)
        print("ERROR OCCURED", res.status_code)
    game_info = json.loads(res.text)
    return (game_info, res.status_code)


#게임의 더욱 상세한 타임라인. 하나하나 다 기록되어 있는것같음
def get_match_timeline_from_game_id(game_id):
    url = 'https://asia.api.riotgames.com/lol/match/v5/matches/'+game_id+'/timeline'
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.118 Whale/2.11.126.23 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://developer.riotgames.com",
        "X-Riot-Token": token
    }
    res = requests.get(url, headers=header)
    if res.status_code != 200:
        print(res.text)
        print('get match timeline from game id', game_id)
        print("ERROR OCCURED", res.status_code)
    timeline = json.loads(res.text)
    return (timeline, res.status_code)

#브실골플다 유저 리스트 받아옴
def get_user_list_from_tier(tier, division, page):
    url = 'https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/'+tier+'/'+division+'?page=' + str(page)
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.118 Whale/2.11.126.23 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://developer.riotgames.com",
        "X-Riot-Token": token
    }
    res = requests.get(url, headers=header)
    if res.status_code != 200:
        print(res.text)
        print('get iser list from tier', tier, division, page)
        print("ERROR OCCURED", res.status_code)
    normal_user_list = json.loads(res.text)
    return (normal_user_list, res.status_code)


def log(error):
    f = open("./log.txt", 'a')
    f.write(error + '\n')
    f.close()


def game_log(game_name):
    f = open("./game_log.txt", 'a')
    f.write(game_name + '\n')
    f.close()

# 플래티넘, 다이아몬드 유저 대전기록 저장
tier_list = ['PLATINUM', 'DIAMOND']
division_list = ['I', 'II', 'III', 'IV']
for tier in tier_list:
    for division in division_list:
        for page in range(1, 100000):
            df = pd.DataFrame()
            now_user_list = get_user_list_from_tier(tier, division, page)
            if len(now_user_list[0]) == 0:
                break  # 페이지 끝났을 때 여기임.
            for user in now_user_list[0]:
                try:
                    name = user['summonerName']
                    puuid = get_puuid_by_id(name)[0]
                    game_list = get_matches_from_puuid(puuid, 10)
                except Exception as e:
                    log(str(e) + " " + user['summonerName'] + '\n')
                    continue  # puuid 없는 아이디는 넘어가야지.
                time.sleep(2)
                for game in game_list[0]:
                    now_game = get_match_detail_from_game_id(game)  # 여기선 에러 나는 부분이 없다.
                    if now_game[1] == 200:  # 유효한 응답
                        real_game = now_game[0]
                        if real_game['info']['gameVersion'][:5] == now_version:  # ok
                            if real_game['info']['gameType'] == 'MATCHED_GAME':  # 솔랭이면 보내기
                                temp_df = json_normalize(real_game['info'])
                                df = pd.concat([df, temp_df]).reset_index(drop=True)  # append가 아닌 concat의 이유
                        else:  # 버전 지난 게임들임
                            break
                    time.sleep(1)
            df.to_json("./matches/" + tier + "_" + division + "_" + str(page) + ".json")

# 마그챌 유저 대전기록 저장
tier_list1 = ['master', 'grandmaster', 'challenger']
for tier in tier_list1:
    df = pd.DataFrame()
    now_user_list = get_cgmm_user(tier)
    for user in now_user_list[0]['entries']:
        try:
            name = user['summonerName']
            puuid = get_puuid_by_id(name)[0]
            game_list = get_matches_from_puuid(puuid, 10)
        except Exception as e:
            log(str(e) + " " + user['summonerName'] + '\n')
            continue  # puuid 없는 아이디는 넘어가야지.
        time.sleep(2)
        for game in game_list[0]:
            now_game = get_match_detail_from_game_id(game)  # 여기선 에러 나는 부분이 없다.
            if now_game[1] == 200:  # 유효한 응답
                real_game = now_game[0]
                if real_game['info']['gameVersion'][:5] == now_version:  # ok
                    if real_game['info']['gameType'] == 'MATCHED_GAME':  # 솔랭이면 보내기
                        temp_df = json_normalize(real_game['info'])
                        df = pd.concat([df, temp_df]).reset_index(drop=True)  # append가 아닌 concat의 이유
                else:  # 버전 지난 게임들임
                    break
            time.sleep(1)
    df.to_json("./matches/" + tier + ".json")

