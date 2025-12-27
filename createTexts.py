import pandas as pd
import os
import csv
import time
import json
import concurrent.futures
from openai import OpenAI
from read_demo import makeCSV
import config # 引入 config 确保统一

OPENAI_API_KEY = None
OPENAI_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
MODEL_NAME = "qwen-max" 
MAX_WORKERS = 8 

CSV_FILES = {
    "smoke": "烟雾弹详细信息.csv",
    "inferno": "燃烧弹详细信息.csv",
    "other": "其他投掷物详细信息.csv"
}

def setAPI_KEY(api_key):
    global OPENAI_API_KEY
    if api_key: OPENAI_API_KEY = api_key
    else: OPENAI_API_KEY = os.getenv("DASHSCOPE_API_KEY")

def clean_json_text(text):
    text = text.strip()
    if text.startswith("```json"): text = text[7:]
    if text.startswith("```"): text = text[3:]
    if text.endswith("```"): text = text[:-3]
    return text.strip()

def analyze_grenade_with_llm(row_data):
    if not OPENAI_API_KEY: return "", "", ""
    client = OpenAI(api_key=OPENAI_API_KEY, base_url=OPENAI_BASE_URL)
    
    grenade_type = str(row_data.get('投掷物类型', '道具'))
    thrower = str(row_data.get('投掷人', '未知选手'))
    land_area = str(row_data.get('落点所在范围', '未知区域'))
    
    prompt = f"""
    解说CS2投掷物事件：
    选手：{thrower}
    投掷：{grenade_type}
    落点：{land_area}
    输出JSON: {{"short": "...", "medium": "...", "long": "..."}}
    """
    
    for _ in range(3):
        try:
            resp = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[{"role": "system", "content": "你是一个CS2解说。请输出标准JSON。"}, {"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            content = resp.choices[0].message.content
            res = json.loads(clean_json_text(content))
            return res.get("short", ""), res.get("medium", ""), res.get("long", "")
        except: time.sleep(0.5)
            
    return f"{thrower}{land_area}投掷{grenade_type}", "", ""

def run_grenade_analysis(demo_path=None, test_mode=False):
    print("💣 [Grenade] 开始道具分析...")
    
    if demo_path and os.path.exists(demo_path):
        try: makeCSV(demo_path) 
        except: pass

    base_name = os.path.splitext(os.path.basename(demo_path))[0] if demo_path else "demo"
    output_dir = os.path.join("data", base_name)
    cache_dir = os.path.join(output_dir, "cache")
    if not os.path.exists(cache_dir): os.makedirs(cache_dir)
    cache_path = os.path.join(cache_dir, "grenade_gen_cache.csv")

    if os.path.exists(cache_path):
        try:
            df = pd.read_csv(cache_path)
            if not df.empty:
                if test_mode and 'round_num' in df.columns:
                    df = df[df['round_num'] == 1]
                return df
        except: pass

    all_grenades = []
    for fname in CSV_FILES.values():
        if os.path.exists(fname):
            try:
                df = pd.read_csv(fname, encoding='utf-8-sig')
                grenades = df.to_dict('records')
                if test_mode and '回合数' in df.columns:
                    grenades = [g for g in grenades if g.get('回合数') == 1]
                all_grenades.extend(grenades)
            except: pass
            
    if not all_grenades: return pd.DataFrame()

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(analyze_grenade_with_llm, item): item for item in all_grenades}
        
        for future in concurrent.futures.as_completed(future_map):
            item = future_map[future]
            try:
                s, m, l = future.result()
                
                # 🔥 优先使用 read_demo 算好的 start_time (已经是 /64 的结果)
                start_time = float(item.get("start_time", 0.0))
                # 兜底：如果没算好，手动除以 64
                if start_time == 0:
                    start_time = float(item.get("tick时间戳", 0)) / float(config.TICKRATE)

                results.append({
                    "event_id": f"grenade_{item.get('tick时间戳')}",
                    "round_num": item.get("回合数"),
                    "start_time": start_time, 
                    "priority": 3,
                    "short_text_neutral": s,
                    "medium_text_neutral": m,
                    "long_text_neutral": l,
                    "event_type": "grenade"
                })
            except: pass

    df_res = pd.DataFrame(results)
    if not df_res.empty:
        df_res.to_csv(cache_path, index=False, encoding='utf-8-sig')
    
    return df_res