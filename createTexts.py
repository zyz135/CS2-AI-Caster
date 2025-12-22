import pandas as pd
import json
from pathlib import Path
from openai import OpenAI
import time
import os
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from read_demo import makeCSV

# ===================== 全局配置 =====================
OPENAI_API_KEY = None
OPENAI_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
MODEL_NAME = "qwen-max" 

# 并发数量
MAX_WORKERS = 8 

CSV_PATHS = {
    "smoke": "烟雾弹详细信息.csv",
    "inferno": "燃烧弹详细信息.csv",
    "other": "其他投掷物详细信息.csv"
}

def setAPI_KEY(api_key):
    global OPENAI_API_KEY
    if api_key: OPENAI_API_KEY = api_key
    else: OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

def load_grenades_data():
    df_combined = pd.DataFrame()
    for _, csv_path in CSV_PATHS.items():
        if not Path(csv_path).exists(): continue
        try:
            df = pd.read_csv(csv_path, encoding="utf-8-sig")
            req_cols = ["entity_id","投掷人", "落点所在范围", "投掷物类型", "tick时间戳","回合数"]
            if not set(req_cols).issubset(df.columns): continue
            
            df = df.dropna(subset=["投掷人", "落点所在范围", "tick时间戳"])
            df = df[df["tick时间戳"] > 0] 
            
            # 构造唯一ID，用于去重判断
            df['unique_key'] = df['回合数'].astype(str) + "_grenade_" + df['entity_id'].astype(str)
            
            df_combined = pd.concat([df_combined, df], ignore_index=True)
        except: pass
        
    if df_combined.empty: return []
    df_combined = df_combined.sort_values(by="tick时间戳")
    return df_combined.to_dict('records')

def process_single_grenade(client, item):
    """
    单个投掷物处理函数
    """
    thrower = item["投掷人"]
    land_area = item["落点所在范围"]
    type_cn = item.get("投掷物类型", "投掷物")
    side = item.get("投掷人所在队伍/阵营", "未知")

    system_prompt = "你是CS2解说。仅描述投掷物战术。输出格式：短版---中版---长版"
    user_prompt = f"投掷人:{thrower}, 阵营:{side}, 落点:{land_area}, 类型:{type_cn}"

    try:
        response = client.chat.completions.create(
            model=MODEL_NAME, 
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ], 
            temperature=0.7
        )
        content = response.choices[0].message.content.strip()
        parts = [p.strip() for p in content.split("---") if p.strip()]
        
        short = parts[0] if len(parts) > 0 else f"{thrower}投掷{type_cn}。"
        medium = parts[1] if len(parts) > 1 else short
        long = parts[2] if len(parts) > 2 else medium
        return short, medium, long
    except Exception as e:
        return f"{thrower}投掷{type_cn}", f"{thrower}在{land_area}投掷{type_cn} (API Error)", f"{thrower}投掷{type_cn}"

def run_grenade_analysis(demo_path):
    print(f"💣 [Grenade] 开始分析道具: {demo_path}")
    makeCSV(demo_path)
    
    if not OPENAI_API_KEY:
        print("   [Grenade] 无 API Key，跳过生成")
        return pd.DataFrame()

    # 定义缓存文件路径 (保存到 data/Demo名/ 下)
    base_name = os.path.splitext(os.path.basename(demo_path))[0]
    output_dir = os.path.join("data", base_name)
    os.makedirs(output_dir, exist_ok=True)
    cache_file = os.path.join(output_dir, "grenade_gen_cache.csv")

    # 1. 读取已有缓存
    existing_ids = set()
    if os.path.exists(cache_file):
        try:
            df_cache = pd.read_csv(cache_file, encoding="utf-8-sig")
            if 'event_id' in df_cache.columns:
                existing_ids = set(df_cache['event_id'].astype(str))
            print(f"   [Grenade] ♻️ 发现缓存: 已有 {len(existing_ids)} 条记录，将跳过生成")
        except: pass

    client = OpenAI(api_key=OPENAI_API_KEY, base_url=OPENAI_BASE_URL)
    grenade_data = load_grenades_data()
    
    if not grenade_data:
        print("   [Grenade] 无有效投掷数据")
        return pd.DataFrame()

    # 2. 过滤掉已经生成的
    tasks = []
    for item in grenade_data:
        event_id = f"{item['回合数']}_grenade_{item['entity_id']}"
        if event_id not in existing_ids:
            tasks.append(item)
    
    if not tasks:
        print("   [Grenade] 所有道具均已生成过，直接读取缓存。")
        return pd.read_csv(cache_file, encoding="utf-8-sig")

    print(f"   [Grenade] 需生成 {len(tasks)} 条 (跳过 {len(existing_ids)} 条)...")
    
    # 3. 准备写入文件 (如果是新文件，写入表头)
    file_exists = os.path.exists(cache_file)
    csv_fields = ["event_id", "round_num", "start_time", "priority", 
                  "short_text_neutral", "medium_text_neutral", "long_text_neutral", "event_type"]
    
    # 打开文件准备追加
    with open(cache_file, mode='a', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        if not file_exists:
            writer.writeheader()
        
        # 4. 开启多线程
        completed_count = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_item = {
                executor.submit(process_single_grenade, client, item): item 
                for item in tasks
            }
            
            for future in as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    short, medium, long = future.result()
                    
                    raw_type = str(item["投掷物类型"])
                    prio = 3
                    if "烟" in raw_type or "smoke" in raw_type.lower(): prio = 5
                    elif "燃" in raw_type or "inferno" in raw_type.lower(): prio = 4
                    elif "闪" in raw_type or "flash" in raw_type.lower(): prio = 6
                    
                    row = {
                        "event_id": f"{item['回合数']}_grenade_{item['entity_id']}",
                        "round_num": item["回合数"],
                        "start_time": item["tick时间戳"] / 128.0, 
                        "priority": prio,
                        "short_text_neutral": short,
                        "medium_text_neutral": medium,
                        "long_text_neutral": long,
                        "event_type": "grenade"
                    }
                    
                    # === 实时写入 ===
                    writer.writerow(row)
                    f.flush() # 强制刷新缓冲区，确保写入硬盘
                    
                    completed_count += 1
                    if completed_count % 10 == 0:
                        print(f"      🚀 进度: {completed_count}/{len(tasks)} ...")
                        
                except Exception as e:
                    print(f"      ❌ 单条处理失败: {e}")

    print(f"✅ [Grenade] 全部完成！")
    # 最后重新读取完整文件返回，确保顺序
    return pd.read_csv(cache_file, encoding="utf-8-sig")

def batch_generate_commentary(): return pd.DataFrame()