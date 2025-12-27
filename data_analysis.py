import pandas as pd
import os
import random
import uuid
import json
import concurrent.futures
from openai import OpenAI
import time
import csv

# 全局配置
LLM_API_KEY = None
def setAPI(API_KEY):
    global LLM_API_KEY
    LLM_API_KEY = API_KEY

LLM_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
MAX_WORKERS = 8 
SKIP_SECONDS = 20.0 

def clean_json_text(text):
    text = text.strip()
    if text.startswith("```json"): text = text[7:]
    if text.startswith("```"): text = text[3:]
    if text.endswith("```"): text = text[:-3]
    s = text.find('{')
    e = text.rfind('}')
    if s != -1 and e != -1: text = text[s:e+1]
    return text.strip()

def generate_prompt_from_data(slice_df, r_num, t_rel):
    alive = slice_df[slice_df['health'] > 0]
    t_p = alive[alive['side'] == 'T']
    ct_p = alive[alive['side'] == 'CT']
    if t_p.empty and ct_p.empty: return None

    def get_pos(df): return ", ".join([f"{r['name']}@{r.get('location_name', r.get('area','?'))}" for _, r in df.iterrows()])
    
    prompt = f"第{r_num}回合，进行到{int(t_rel)}秒。\n"
    prompt += f"T位置: {get_pos(t_p)}\nCT位置: {get_pos(ct_p)}\n"
    prompt += "分析双方意图(Short:10字, Medium:30字)。"
    return prompt

def process_slice_task(slice_df, r_num):
    min_tick = slice_df['tick'].min()
    if 'second' in slice_df.columns:
        t_rel = slice_df['second'].min()
    else: return None

    if t_rel < SKIP_SECONDS: return None

    prompt = generate_prompt_from_data(slice_df, r_num, t_rel)
    if not prompt or not LLM_API_KEY: return None
    
    client = OpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)
    for _ in range(2):
        try:
            # 不用 response_format，兼容性更好
            resp = client.chat.completions.create(
                model="qwen-max",
                messages=[{"role": "system", "content": "你是CS2战术分析师。输出JSON: {\"short\":\"...\", \"medium\":\"...\", \"long\":\"...\"}"}, 
                          {"role": "user", "content": prompt}]
            )
            raw = resp.choices[0].message.content
            try:
                data = json.loads(clean_json_text(raw))
                short = data.get("short", "")
                medium = data.get("medium", "")
                long = data.get("long", "")
            except:
                short = raw[:30]
                medium = raw
                long = raw

            return {
                "event_id": str(uuid.uuid4()),
                "round_num": r_num,
                "tick": int(min_tick),
                "priority": 4, 
                "short_text_neutral": short,
                "medium_text_neutral": medium,
                "long_text_neutral": long,
                "event_type": "tactical"
            }
        except: time.sleep(0.5)
    return None

def run_tactical_analysis(df_pretreatment, output_dir=None, target_rounds=None, test_mode=False):
    print(f"🧠 [Tactical] 开始战术分析...")
    
    if df_pretreatment is None or df_pretreatment.empty: 
        print("   ⚠️ [Tactical] 预处理数据为空，跳过")
        return pd.DataFrame()

    cache_path = None
    if output_dir:
        cache_dir = os.path.join(output_dir, "cache")
        os.makedirs(cache_dir, exist_ok=True)
        cache_path = os.path.join(cache_dir, "tactical_gen_cache_v2.csv")
        
        # 缓存检查
        if os.path.exists(cache_path):
            try:
                df = pd.read_csv(cache_path)
                if not df.empty:
                    if test_mode and 'round_num' in df.columns:
                        if 1 in df['round_num'].values:
                            return df[df['round_num'] == 1]
                    else:
                        return df
            except: pass
        # 清除旧缓存
        try: os.remove(cache_path)
        except: pass

    tasks = []
    print(f"   🚀 生成任务队列...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for r_num, df_round in df_pretreatment.groupby("round_num"):
            if test_mode and r_num != 1: continue
            
            if 'second' in df_round.columns:
                min_sec = df_round['second'].min()
                max_sec = df_round['second'].max()
                curr = min_sec
                # 间隔 15 秒
                while curr < max_sec:
                    slice_df = df_round[(df_round['second'] >= curr) & (df_round['second'] < curr + 1.0)]
                    if not slice_df.empty:
                        tasks.append(executor.submit(process_slice_task, slice_df, r_num))
                    curr += 15.0

    results = []
    # 如果有 cache path，先写入表头
    if output_dir and cache_path:
        pd.DataFrame(columns=["event_id", "round_num", "tick", "priority", "short_text_neutral", "medium_text_neutral", "long_text_neutral", "event_type"]).to_csv(cache_path, index=False, encoding="utf-8-sig")

    count = 0
    for f in concurrent.futures.as_completed(tasks):
        try:
            res = f.result()
            if res: 
                results.append(res)
                if output_dir and cache_path:
                    pd.DataFrame([res]).to_csv(cache_path, mode='a', index=False, header=False, encoding="utf-8-sig")
                count += 1
                if count % 10 == 0: print(f"      [Tactical] 进度: {count}/{len(tasks)}")
        except: pass
        
    df_res = pd.DataFrame(results)
    print(f"✅ [Tactical] 完成，生成 {len(df_res)} 条")
    return df_res