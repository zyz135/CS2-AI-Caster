import pandas as pd
import numpy as np
import os
import time
import csv
from awpy import Demo
from openai import OpenAI
import warnings

warnings.filterwarnings('ignore')

# ===================== 全局配置 =====================
OPENAI_API_KEY = os.getenv("DASHSCOPE_API_KEY") or os.getenv("OPENAI_API_KEY")
BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
MODEL_NAME = "qwen-max"

def get_weapon_name(weapon_raw):
    # 简单的武器名映射
    w_map = {
        "ak47": "AK-47", "m4a1": "M4A4", "m4a1_silencer": "M4A1-S",
        "awp": "AWP", "deagle": "沙鹰", "usp_silencer": "USP",
        "glock": "格洛克", "inferno": "燃烧弹", "hegrenade": "手雷",
        "knife": "刀"
    }
    return w_map.get(str(weapon_raw).lower(), weapon_raw)

def analyze_kill_with_llm(client, event_data):
    """调用大模型生成击杀解说"""
    if not client: return "", "", ""
    
    system_prompt = "你是CS2专业解说。请根据击杀信息生成解说文本。输出格式：短版---中版---长版"
    
    # 构建描述
    desc = f"时间:{event_data['round_num']}回合 {event_data['time']}秒. "
    desc += f"击杀者:{event_data['attacker']} ({event_data['attacker_side']}), 武器:{event_data['weapon']}. "
    desc += f"死者:{event_data['victim']} ({event_data['victim_side']}). "
    if event_data['is_headshot']: desc += "爆头. "
    if event_data['is_wallbang']: desc += "穿墙. "
    if event_data['is_blind']: desc += "被致盲. "
    if event_data['is_noscope']: desc += "盲狙. "
    if event_data['attacker_blind']: desc += "击杀者被白. "
    
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": desc}
            ],
            temperature=0.7
        )
        content = response.choices[0].message.content.strip()
        parts = [p.strip() for p in content.split("---") if p.strip()]
        
        short = parts[0] if len(parts) > 0 else f"{event_data['attacker']}击杀{event_data['victim']}"
        medium = parts[1] if len(parts) > 1 else short
        long = parts[2] if len(parts) > 2 else medium
        return short, medium, long
    except Exception as e:
        print(f"      ❌ LLM Error: {e}")
        return "", "", ""

def extract_basic_events(dem):
    """从 Demo 中提取原始击杀事件 (适配特定列名)"""
    kills = []
    
    # 1. 获取 Kills 数据
    if hasattr(dem, 'kills') and hasattr(dem.kills, 'to_pandas'):
        df_kills = dem.kills.to_pandas()
    elif hasattr(dem, 'kills'):
        df_kills = pd.DataFrame(dem.kills)
    else:
        return []

    if df_kills.empty: return []

    # 2. 获取 Rounds 数据 (用于计算时间)
    round_starts = {}
    if hasattr(dem, 'rounds'):
        rounds = dem.rounds.to_pandas() if hasattr(dem.rounds, 'to_pandas') else pd.DataFrame(dem.rounds)
        
        # === 🟢 修复点 1: 使用 'start' 列而不是 'start_tick' ===
        if not rounds.empty and 'start' in rounds.columns:
            round_starts = rounds.set_index('round_num')['start'].to_dict()
        elif not rounds.empty and 'start_tick' in rounds.columns:
            round_starts = rounds.set_index('round_num')['start_tick'].to_dict()

    tickrate = 128
    if hasattr(dem, 'tickrate'): tickrate = dem.tickrate
    elif hasattr(dem, 'header') and 'tickrate' in dem.header: tickrate = dem.header['tickrate']

    for _, row in df_kills.iterrows():
        try:
            r_num = row['round_num']
            tick = row['tick']
            
            # 计算秒数
            start_tick = round_starts.get(r_num, 0)
            if start_tick == 0: 
                # 尝试用第一行的 tick 兜底，或者直接跳过
                continue 
            
            sec = (tick - start_tick) / tickrate
            # 允许少量负数(如开局前的击杀)，但一般过滤掉太离谱的
            if sec < -5: continue 
            if sec < 0: sec = 0.0

            # 提取基础信息
            attacker = row.get('attacker_name')
            victim = row.get('victim_name')
            
            if pd.isna(attacker) or pd.isna(victim): continue
            
            # === 🟢 修复点 2: 映射正确的列名 ===
            # is_wallbang -> penetrated (大于0为穿墙)
            # is_headshot -> headshot
            # is_noscope -> noscope
            # attacker_blind -> attackerblind
            
            is_wallbang = False
            if 'penetrated' in row: is_wallbang = (row['penetrated'] > 0)
            
            event = {
                'round_num': r_num,
                'tick': tick,
                'start_time': float(f"{sec:.2f}"),
                'attacker': attacker,
                'victim': victim,
                'attacker_side': row.get('attacker_side', 'Unknown'),
                'victim_side': row.get('victim_side', 'Unknown'),
                'weapon': get_weapon_name(row.get('weapon', 'unknown')),
                
                # 使用你的列名
                'is_headshot': row.get('headshot', False),
                'is_wallbang': is_wallbang,
                'is_blind': row.get('attackerblind', False), # 注意：这里通常是指击杀者是否致盲
                'is_noscope': row.get('noscope', False),
                'attacker_blind': row.get('attackerblind', False),
                'event_type': 'kill'
            }
            
            # 唯一指纹
            event['unique_key'] = f"R{r_num}_{attacker}_{victim}_{tick}"
            
            kills.append(event)
        except Exception as e:
            continue
        
    return kills

def process_dem_file(demo_path, api_key=None, verbose=True):
    """主入口：处理 Demo 并生成击杀解说"""
    global OPENAI_API_KEY
    if api_key: OPENAI_API_KEY = api_key
    
    if verbose: print(f"🔍 [Kill] 解析 Demo: {os.path.basename(demo_path)}")
    
    # 1. 定义缓存路径
    base_name = os.path.splitext(os.path.basename(demo_path))[0]
    output_dir = os.path.join("data", base_name)
    os.makedirs(output_dir, exist_ok=True)
    cache_file = os.path.join(output_dir, "kill_gen_cache.csv")
    
    # 2. 读取缓存
    existing_keys = set()
    if os.path.exists(cache_file):
        try:
            df_cache = pd.read_csv(cache_file)
            if 'unique_key' in df_cache.columns:
                existing_keys = set(df_cache['unique_key'].astype(str))
            if verbose: print(f"   [Kill] ♻️ 发现缓存: 已有 {len(existing_keys)} 条记录")
        except: pass

    # 3. 解析 Demo 获取原始事件
    try:
        dem = Demo(demo_path, verbose=False)
        dem.parse()
        raw_events = extract_basic_events(dem)
    except Exception as e:
        print(f"   ❌ [Kill] Demo 解析失败: {e}")
        return pd.DataFrame()

    if not raw_events:
        print("   [Kill] 未找到有效击杀事件 (可能列名匹配仍有问题)")
        return pd.DataFrame()

    # 4. 过滤已生成的任务
    tasks = [e for e in raw_events if e['unique_key'] not in existing_keys]
    
    if not tasks:
        print("   [Kill] 所有击杀均已生成过，直接读取缓存。")
        return pd.read_csv(cache_file)

    if verbose: print(f"   [Kill] 需生成 {len(tasks)} 条 (跳过 {len(existing_keys)} 条)...")

    # 5. 初始化 Client
    client = None
    if OPENAI_API_KEY:
        client = OpenAI(api_key=OPENAI_API_KEY, base_url=BASE_URL)
    else:
        print("   ⚠️ [Kill] 无 API Key，将生成默认模板文本")

    # 6. 逐条处理并实时保存
    csv_fields = [
        'event_id', 'round_num', 'start_time', 'priority', 
        'short_text_neutral', 'medium_text_neutral', 'long_text_neutral', 
        'event_type', 'unique_key'
    ]
    
    file_exists = os.path.exists(cache_file)
    
    with open(cache_file, mode='a', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        if not file_exists:
            writer.writeheader()
            
        for idx, event in enumerate(tasks):
            # 调用 API (或生成默认)
            short, medium, long = "", "", ""
            if client:
                short, medium, long = analyze_kill_with_llm(client, {
                    'round_num': event['round_num'],
                    'time': event['start_time'],
                    'attacker': event['attacker'],
                    'victim': event['victim'],
                    'attacker_side': event['attacker_side'],
                    'victim_side': event['victim_side'],
                    'weapon': event['weapon'],
                    'is_headshot': event['is_headshot'],
                    'is_wallbang': event['is_wallbang'],
                    'is_blind': event['is_blind'],
                    'is_noscope': event['is_noscope'],
                    'attacker_blind': event['attacker_blind']
                })
                # 优先级逻辑
                prio = 6
                if event['is_noscope'] or (event['is_headshot'] and 'deagle' in str(event['weapon']).lower()): prio = 7
                if "刀" in str(event['weapon']): prio = 8
            else:
                short = f"{event['attacker']}击杀{event['victim']}"
                medium = short
                long = short
                prio = 6

            if not short: short = f"{event['attacker']}击杀{event['victim']}"

            # 构造行数据
            row = {
                'event_id': f"kill_{event['unique_key']}",
                'round_num': event['round_num'],
                'start_time': event['start_time'],
                'priority': prio,
                'short_text_neutral': short,
                'medium_text_neutral': medium if medium else short,
                'long_text_neutral': long if long else medium,
                'event_type': 'kill',
                'unique_key': event['unique_key']
            }
            
            # 实时写入
            writer.writerow(row)
            f.flush()
            
            if verbose and (idx + 1) % 10 == 0:
                print(f"      🚀 [Kill] 进度: {idx + 1}/{len(tasks)}")

    if verbose: print(f"✅ [Kill] 全部完成！")
    
    # 重新读取并排序返回
    final_df = pd.read_csv(cache_file)
    return final_df.sort_values(by=['round_num', 'start_time'])