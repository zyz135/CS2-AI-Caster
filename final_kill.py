import pandas as pd
import numpy as np
import os
import time
import json
import uuid
import concurrent.futures
from openai import OpenAI
import warnings
import config # 引入 config

warnings.filterwarnings('ignore')

OPENAI_API_KEY = os.getenv("DASHSCOPE_API_KEY") 
BASE_URL = "[https://dashscope.aliyuncs.com/compatible-mode/v1](https://dashscope.aliyuncs.com/compatible-mode/v1)"
MODEL_NAME = "qwen3-max"
MAX_WORKERS = 10 

def analyze_kill_with_llm(client, event_data):
    if not client: return {"short": f"{event_data['attacker']}击杀{event_data['victim']}", "medium":"", "long":""}
    desc = f"击杀: {event_data['attacker']} 用 {event_data['weapon']} 击杀 {event_data['victim']}."
    if event_data['is_headshot']: desc += "爆头."
    
    for _ in range(3):
        try:
            resp = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[{"role": "system", "content": "你是CS2解说。输出JSON: {\"short\":\"...\", \"medium\":\"...\", \"long\":\"...\"}"}, {"role": "user", "content": desc}],
                response_format={"type": "json_object"}
            )
            content = resp.choices[0].message.content
            if content.startswith("```json"): content = content[7:-3]
            return json.loads(content)
        except: time.sleep(0.5)
    return {"short": desc, "medium": "", "long": ""}

def process_single_kill(client, evt):
    res = analyze_kill_with_llm(client, evt)
    evt['short_text_neutral'] = res.get('short', '')
    evt['medium_text_neutral'] = res.get('medium', '')
    evt['long_text_neutral'] = res.get('long', '')
    evt['priority'] = 6 
    evt['event_type'] = 'kill'
    return evt

def process_dem_file(demo_path, test_mode=False):
    print(f"🔫 [Kill] 开始分析击杀...")
    
    base_name = os.path.splitext(os.path.basename(demo_path))[0]
    output_dir = os.path.join("data", base_name)
    cache_dir = os.path.join(output_dir, "cache")
    if not os.path.exists(cache_dir): os.makedirs(cache_dir)
    cache_path = os.path.join(cache_dir, "kill_gen_cache.csv")
    
    if os.path.exists(cache_path):
        try:
            df = pd.read_csv(cache_path)
            if not df.empty:
                if test_mode and 'round_num' in df.columns:
                    df = df[df['round_num'] == 1]
                return df
        except: pass

    from awpy import Demo
    dem = Demo(demo_path)
    dem.parse()
    
    kills = dem.kills
    if not isinstance(kills, pd.DataFrame): kills = kills.to_pandas()
    if kills.empty: return pd.DataFrame()
    
    processed_events = []
    client = OpenAI(api_key=OPENAI_API_KEY, base_url=BASE_URL) if OPENAI_API_KEY else None

    for k in kills.to_dict('records'):
        r_num = k.get('round', k.get('round_num', 0))
        if r_num == 0 or (test_mode and r_num != 1): continue

        tick = k.get('tick', 0)
        evt = {
            'round_num': int(r_num),
            # 🔥🔥🔥 强制使用 64 Tick 🔥🔥🔥
            'start_time': tick / float(config.TICKRATE),
            'attacker': k.get('attacker_name', 'Unknown'),
            'victim': k.get('victim_name', 'Unknown'),
            'weapon': k.get('weapon', 'Unknown'),
            'is_headshot': bool(k.get('headshot', False)),
            'unique_key': str(uuid.uuid4())
        }
        processed_events.append(evt)

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_evt = {executor.submit(process_single_kill, client, evt): evt for evt in processed_events}
        for future in concurrent.futures.as_completed(future_to_evt):
            try: results.append(future.result())
            except: pass
            
    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values(by=['round_num', 'start_time']) # 排序
        df.to_csv(cache_path, index=False, encoding='utf-8-sig')
        
    return df