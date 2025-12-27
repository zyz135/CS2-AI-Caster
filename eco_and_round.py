from awpy import Demo
import polars as pl
import pandas as pd
import os
import json
import threading
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI
from dotenv import load_dotenv
import config  # 引入配置

csv_lock = threading.Lock()
FORCE_TICKRATE = 64.0

ITEM_NAME_CN = {
    "glock": "格洛克", "hkp2000": "P2000", "usp_silencer": "USP消音", "p250": "P250",
    "elite": "双持贝瑞塔", "fiveseven": "FN57", "tec9": "Tec-9", "cz75a": "CZ75",
    "deagle": "沙漠之鹰", "revolver": "R8左轮", "mac10": "MAC-10", "mp9": "MP9",
    "mp7": "MP7", "mp5sd": "MP5消音", "ump45": "UMP-45", "p90": "P90", "bizon": "PP野牛",
    "famas": "法玛斯", "galilar": "加利尔", "m4a1": "M4A1", "m4a1_silencer": "M4A1消音",
    "ak47": "AK-47", "sg556": "SG553", "aug": "AUG", "ssg08": "SSG08", "awp": "AWP",
    "scar20": "SCAR-20", "g3sg1": "G3SG1", "nova": "新星", "xm1014": "XM1014",
    "mag7": "MAG-7", "sawedoff": "截短霰弹枪", "m249": "M249", "negev": "内格夫",
    "vest": "防弹衣", "vesthelm": "防弹衣+头盔", "defuser": "拆弹器", "taser": "电击枪",
    "hegrenade": "高爆手雷", "flashbang": "闪光弹", "smokegrenade": "烟雾弹",
    "molotov": "燃烧瓶", "incgrenade": "燃烧弹", "decoy": "诱饵弹", "c4": "C4炸弹",
    "knife": "刀", "knife_t": "刀",
}

REASON_CN = {
    "ct_killed": "CT全灭", "t_killed": "T全灭", "bomb_exploded": "炸弹爆炸",
    "bomb_defused": "炸弹拆除", "time_ran_out": "时间耗尽",
    "ct_surrender": "CT投降", "t_surrender": "T投降",
}

def get_item_cn(item_en: str) -> str:
    return ITEM_NAME_CN.get(item_en.lower(), item_en)

def get_reason_cn(reason: str) -> str:
    return REASON_CN.get(reason, reason)

def clean_json_text(text):
    text = text.strip()
    # 移除 markdown
    if text.startswith("```"): 
        text = text.split("\n", 1)[-1]
        if text.endswith("```"): text = text[:-3]
    # 尝试提取JSON部分
    s = text.find('{')
    e = text.rfind('}')
    if s != -1 and e != -1: text = text[s:e+1]
    return text.strip()

def process_single_eco_task(client, system_prompt, user_prompt, metadata, cache_file):
    short, medium, long = "", "", ""
    raw_content = ""
    
    try:
        # 🔥🔥🔥 移除 response_format，改用普通文本生成，兼容性更好 🔥🔥🔥
        resp = client.chat.completions.create(
            model="qwen-max",
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}]
        )
        raw_content = resp.choices[0].message.content
        
        # 尝试解析
        try:
            data = json.loads(clean_json_text(raw_content))
            short = data.get("short", "")
            medium = data.get("medium", "")
            long = data.get("long", "")
        except:
            # 🔥 兜底：解析失败直接用原文
            clean_txt = raw_content.replace('\n', ' ').replace('"', '')
            short = clean_txt[:30]
            medium = clean_txt
            long = clean_txt

    except Exception as e:
        print(f"      ⚠️ [Economy] LLM Error: {e}")
    
    # 终极兜底：如果还是空的
    if not short:
        # 如果是经济分析，生成简单文本
        if metadata['event_type'] == 2:
            short = f"第{metadata['round_num']}回合开局，双方准备就绪。"
        else:
            short = f"第{metadata['round_num']}回合结束。"
        medium = short
        long = short

    row = {
        "event_id": metadata['event_id'],
        "round_num": metadata['round_num'],
        "start_time": metadata['start_time'],
        "end_time": metadata['end_time'],
        "event_type": metadata['event_type'],
        "priority": metadata['priority'],
        "short_text_neutral": short,
        "medium_text_neutral": medium,
        "long_text_neutral": long
    }
    
    with csv_lock:
        try:
            file_exists = os.path.exists(cache_file) and os.path.getsize(cache_file) > 0
            with open(cache_file, mode='a', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=row.keys())
                if not file_exists: writer.writeheader()
                writer.writerow(row)
        except: pass

def analyze_economy(demo_path: str, enable_llm: bool = True, test_mode: bool = False):
    base_name = os.path.splitext(os.path.basename(demo_path))[0]
    output_dir = os.path.join("data", base_name)
    os.makedirs(output_dir, exist_ok=True)
    cache_file = os.path.join(output_dir, "economy_gen_cache.csv")

    # 缓存检查：如果是测试模式且缓存里有第一回合，直接返回
    if os.path.exists(cache_file):
        try:
            df = pd.read_csv(cache_file, encoding='utf-8-sig')
            if not df.empty:
                if test_mode:
                    if 1 in df['round_num'].values:
                        print("   💰 [Economy] 读取缓存")
                        return df[df['round_num'] == 1]
                else:
                    print("   💰 [Economy] 读取缓存")
                    return df
        except: pass

    # 重新生成前清理旧文件
    if os.path.exists(cache_file):
        try: os.remove(cache_file)
        except: pass

    print(f"💰 [Economy] 解析 Demo (强制64Tick)...")
    demo = Demo(demo_path)
    demo.parse()
    
    tickrate = float(config.TICKRATE)
    map_name = demo.header.get('map_name', 'unknown')
    
    client = None
    if enable_llm:
        load_dotenv()
        key = os.getenv("DASHSCOPE_API_KEY") or os.getenv("OPENAI_API_KEY")
        if key: client = OpenAI(api_key=key, base_url="https://dashscope.aliyuncs.com/compatible-mode/v1")

    # 数据提取
    economy_fields = ["CCSPlayerController.CCSPlayerController_InGameMoneyServices.m_iAccount", "CCSPlayerController.CCSPlayerController_InGameMoneyServices.m_iStartAccount", "team_num"]
    economy_df = pl.from_pandas(demo.parser.parse_ticks(wanted_props=economy_fields)).rename({"CCSPlayerController.CCSPlayerController_InGameMoneyServices.m_iAccount": "remaining_money", "CCSPlayerController.CCSPlayerController_InGameMoneyServices.m_iStartAccount": "start_money"})
    economy_df = economy_df.with_columns(pl.when(pl.col("team_num") == 2).then(pl.lit("T")).when(pl.col("team_num") == 3).then(pl.lit("CT")).otherwise(pl.lit("未知")).alias("side"))
    item_pickup_df = pl.from_pandas(demo.parser.parse_event("item_pickup")).rename({"user_name": "name", "user_steamid": "steamid"})
    kills_df = demo.kills
    rounds_df = demo.rounds
    
    round_ranges = []
    for row in rounds_df.to_dicts():
        round_ranges.append({"round_num": row["round_num"], "start": row["start"], "freeze_end": row["freeze_end"], "end": row["official_end"]})
    
    def get_round_num(tick, ranges):
        for r in ranges:
            if r["start"] <= tick <= r["end"]: return r["round_num"]
        return None
    def is_freeze_time(tick, ranges):
        for r in ranges:
            if r["start"] <= tick <= r["freeze_end"]: return True
        return False

    round_economy_data = []
    for round_info in rounds_df.to_dicts():
        freeze_end_tick = round_info["freeze_end"]
        round_economy = economy_df.filter((pl.col("tick") >= freeze_end_tick - 10) & (pl.col("tick") <= freeze_end_tick + 10))
        if len(round_economy) == 0: continue
        player_economy = round_economy.group_by("name").agg([pl.col("start_money").first(), pl.col("remaining_money").first(), pl.col("steamid").first(), pl.col("side").first()])
        for player in player_economy.to_dicts():
            round_economy_data.append({"round_num": round_info["round_num"], "name": player["name"], "steamid": player["steamid"], "start_money": player["start_money"], "remaining_money": player["remaining_money"], "side": player["side"]})
    round_economy_df = pl.DataFrame(round_economy_data)
    
    item_pickup_with_round = []
    for item in item_pickup_df.to_dicts():
        tick = item["tick"]
        rnum = get_round_num(tick, round_ranges)
        in_freeze = is_freeze_time(tick, round_ranges)
        if rnum is not None:
            item_pickup_with_round.append({**item, "round_num": rnum, "in_freeze_time": in_freeze, "is_purchase": in_freeze})
    purchases_df = pl.DataFrame(item_pickup_with_round).filter(~pl.col("item").is_in(["knife", "knife_t", "c4"]))

    csv_fields = ["event_id", "round_num", "start_time", "end_time", "event_type", "priority", "short_text_neutral", "medium_text_neutral", "long_text_neutral"]
    with open(cache_file, 'w', encoding='utf-8-sig', newline='') as f:
        csv.DictWriter(f, fieldnames=csv_fields).writeheader()

    llm_tasks = []
    print(f"   [Economy] 需处理 {len(rounds_df)} 回合...")

    for round_num in range(1, len(rounds_df) + 1):
        if test_mode and round_num != 1: continue
        
        round_info = rounds_df.filter(pl.col("round_num") == round_num).to_dicts()[0]
        
        eco_time = round_info.get('start', 0) / tickrate
        sum_time = round_info.get('official_end', 0) / tickrate

        round_eco = round_economy_df.filter(pl.col("round_num") == round_num)
        if len(round_eco) == 0: continue

        # 经济 Prompt (防剧透)
        eco_prompt = f"地图: {map_name}\n第 {round_num} 回合开始。\n"
        for side_label, side_filter in [("CT", "CT"), ("T", "T")]:
            side_players = round_eco.filter(pl.col("side") == side_filter).sort("name").to_dicts()
            if not side_players: continue
            eco_prompt += f"{side_label}经济:\n"
            for player in side_players:
                name = player["name"]
                start_money = player["start_money"]
                if round_num > 1:
                    prev_purchases = purchases_df.filter((pl.col("round_num") == round_num - 1) & (pl.col("name") == name) & (pl.col("is_purchase") == True))
                    prev_items = [get_item_cn(i) for i in prev_purchases.select("item").to_series().to_list()]
                else: prev_items = []
                eco_prompt += f"  - {name}: ${start_money}, 上局买: {', '.join(prev_items) if prev_items else '无'}\n"
        eco_prompt += "分析开局经济和起枪情况。JSON字段: short, medium, long"

        # 总结 Prompt (含胜者)
        winner = "CT" if round_info['winner'] == "ct" else "T"
        reason = get_reason_cn(round_info['reason'])
        sum_prompt = f"地图: {map_name}\n第 {round_num} 回合结束。\n获胜: {winner}\n原因: {reason}\n关键击杀:\n"
        round_kills = kills_df.filter(pl.col("round_num") == round_num)
        for kill in round_kills.sort("tick").to_dicts()[:5]:
            attacker = kill.get('attacker_name', '未知')
            victim = kill.get('victim_name', '未知')
            weapon = get_item_cn(kill.get('weapon', ''))
            sum_prompt += f"  - {attacker}({weapon}) 击杀 {victim}\n"
        sum_prompt += "总结本回合。JSON字段: short, medium, long"

        if client:
            meta_eco = {'event_id': f"{round_num}_2_1", 'round_num': round_num, 'start_time': eco_time, 'end_time': eco_time+5, 'event_type': 2, 'priority': 2}
            sys_prompt = "你是CS2解说。请用JSON格式输出: {\"short\":\"...\", \"medium\":\"...\", \"long\":\"...\"}"
            llm_tasks.append((client, sys_prompt, eco_prompt, meta_eco, cache_file))
            
            meta_sum = {'event_id': f"{round_num}_1_1", 'round_num': round_num, 'start_time': sum_time, 'end_time': sum_time+5, 'event_type': 1, 'priority': 1}
            llm_tasks.append((client, sys_prompt, sum_prompt, meta_sum, cache_file))

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_single_eco_task, *t) for t in llm_tasks]
        for _ in as_completed(futures): pass

    return pd.read_csv(cache_file, encoding='utf-8-sig')

def get_events_df(demo_path: str, enable_llm: bool = True, test_mode: bool = False):
    return analyze_economy(demo_path, enable_llm, test_mode)