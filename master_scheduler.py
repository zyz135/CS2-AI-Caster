import pandas as pd
import numpy as np
import os
import sys
import concurrent.futures
from openai import OpenAI
from awpy import Demo 
import config 

# 全局变量
run_tactical_analysis = None
set_tactical_api = None
run_grenade_analysis = None
set_grenade_api = None
process_dem_file = None
get_eco_df = None
extract_specified_player_data_wrapper = None

MERGE_THRESHOLD = 5.0  
MAX_MERGE_COUNT = 3    
COMPRESS_MODEL = "qwen-max"
COMPRESS_PROMPT = "你是一名CS2解说。请将多条解说文案合并为一句简练、紧凑的解说。要求：保留关键信息，字数限制30字以内，口语化。"

print("📦 [System] 加载模块...")
try: from data_analysis import run_tactical_analysis, setAPI as set_tactical_api
except: pass
try: from createTexts import run_grenade_analysis, setAPI_KEY as set_grenade_api
except: pass
try: from read_demo import makeCSV
except: pass
try: from final_kill import process_dem_file
except: pass
try: from eco_and_round import get_events_df as get_eco_df
except: pass
try: from pretreatment import extract_specified_player_data_wrapper
except: pass

class MasterScheduler:
    def __init__(self, demo_path, api_key, test_mode=False):
        self.demo_path = demo_path
        self.api_key = api_key
        self.test_mode = test_mode
        self.base_name = os.path.splitext(os.path.basename(demo_path))[0]
        self.output_dir = os.path.join("data", self.base_name)
        
        self.raw_dir = os.path.join(self.output_dir, "raw")
        self.cache_dir = os.path.join(self.output_dir, "cache")
        self.output_final_dir = os.path.join(self.output_dir, "output")
        for d in [self.output_dir, self.raw_dir, self.cache_dir, self.output_final_dir]:
            if not os.path.exists(d): os.makedirs(d)
            
        self.client = OpenAI(api_key=api_key, base_url="https://dashscope.aliyuncs.com/compatible-mode/v1")
        self.tickrate = float(config.TICKRATE)
        self.time_offsets = self._calculate_half_offsets()

    def _calculate_half_offsets(self):
        print(f"🕒 [Scheduler] 计算时间锚点 (Tickrate=64)...")
        try:
            dem = Demo(self.demo_path)
            dem.parse() 
            rounds = dem.rounds.to_pandas() if hasattr(dem.rounds, 'to_pandas') else pd.DataFrame(dem.rounds)
            
            offset_upper = 0.0
            offset_lower = 0.0
            f_end_col = next((c for c in rounds.columns if 'freeze' in c), 'start')
            
            def get_seconds(row):
                val = row[f_end_col]
                return (val / self.tickrate) - 15.0

            r1 = rounds[rounds['round_num'] == 1]
            if not r1.empty: offset_upper = get_seconds(r1.iloc[0])

            r13 = rounds[rounds['round_num'] == 13]
            if not r13.empty: offset_lower = get_seconds(r13.iloc[0])
            
            print(f"   ✨ R1 偏移: {offset_upper:.2f}s")
            return {"upper": offset_upper, "lower": offset_lower}
        except: return {"upper": 0.0, "lower": 0.0}

    def step1_pretreatment(self):
        print("🔄 [Step 1] 提取基础数据...")
        if extract_specified_player_data_wrapper:
            try:
                self.df_pretreatment = extract_specified_player_data_wrapper(self.demo_path, os.path.join(self.raw_dir, "1_raw_data.csv"))
                if self.test_mode and self.df_pretreatment is not None:
                    self.df_pretreatment = self.df_pretreatment[self.df_pretreatment['round_num'] == 1]
                return True
            except: pass
        return False

    def step2_collect_all_modules(self):
        print("🔄 [Step 2] 并行生成...")
        if set_tactical_api: set_tactical_api(self.api_key)
        if set_grenade_api: set_grenade_api(self.api_key)
        
        all_dfs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = {}
            if process_dem_file: futures[executor.submit(process_dem_file, self.demo_path, self.test_mode)] = "Kill"
            if get_eco_df: futures[executor.submit(get_eco_df, self.demo_path, True, self.test_mode)] = "Eco"
            if run_grenade_analysis: futures[executor.submit(run_grenade_analysis, self.demo_path, self.test_mode)] = "Grenade"
            
            if run_tactical_analysis and self.df_pretreatment is not None:
                futures[executor.submit(run_tactical_analysis, self.df_pretreatment, self.output_dir, None, self.test_mode)] = "Tactical"

            for f in concurrent.futures.as_completed(futures):
                try:
                    df = f.result()
                    if df is not None and not df.empty: all_dfs.append(df)
                except Exception as e: print(f"   ❌ 模块失败: {e}")
        return all_dfs

    def step3_merge(self, all_dfs):
        if not all_dfs: return pd.DataFrame()
        merged = pd.concat(all_dfs, ignore_index=True)
        
        if 'start_time' not in merged.columns:
            if 'tick' in merged.columns: merged['start_time'] = merged['tick'] / self.tickrate
        else:
            if 'tick' in merged.columns: merged['start_time'] = merged['start_time'].fillna(merged['tick'] / self.tickrate)
        
        if self.test_mode and 'round_num' in merged.columns:
            merged = merged[merged['round_num'] == 1]
            
        return merged

    def step4_smart_compression(self, df):
        print("🧠 [Step 4] 智能语义压缩...")
        if df.empty: return df
        df = df.sort_values(['round_num', 'start_time'])
        compressed_rows = []
        buffer = []
        
        for _, row in df.iterrows():
            if row.get('event_type') == 'kill':
                if not buffer: buffer.append(row)
                else:
                    if (row['start_time'] - buffer[-1]['start_time']) < MERGE_THRESHOLD and len(buffer) < MAX_MERGE_COUNT:
                        buffer.append(row)
                    else:
                        compressed_rows.extend(self._flush_buffer(buffer))
                        buffer = [row]
            else:
                if buffer:
                    compressed_rows.extend(self._flush_buffer(buffer))
                    buffer = []
                compressed_rows.append(row)
        
        if buffer: compressed_rows.extend(self._flush_buffer(buffer))
        return pd.DataFrame(compressed_rows)

    def _flush_buffer(self, buffer):
        if not buffer: return []
        if len(buffer) == 1: return buffer
        
        base = buffer[0].copy()
        texts = [b.get('short_text_neutral', '') for b in buffer]
        
        try:
            resp = self.client.chat.completions.create(
                model=COMPRESS_MODEL,
                messages=[{"role": "system", "content": COMPRESS_PROMPT}, {"role": "user", "content": f"合并: {'；'.join(texts)}"}]
            )
            base['medium_text_neutral'] = resp.choices[0].message.content.strip().strip('"')
        except: base['medium_text_neutral'] = "；".join(texts)
        
        base['short_text_neutral'] = base['medium_text_neutral']
        base['span_duration'] = buffer[-1]['start_time'] - buffer[0]['start_time']
        return [base]

    def step5_schedule_and_output(self, df):
        print("⚔️ [Step 5] 最终对齐...")
        schedule = []
        df = df.sort_values(by=['start_time'])
        
        off_upper = self.time_offsets.get("upper", 0.0)
        off_lower = self.time_offsets.get("lower", 0.0)
        cursor_u, cursor_l = 0.0, 0.0

        for _, row in df.iterrows():
            r_num = int(row.get('round_num', 0))
            start_t = float(row.get('start_time', 0))
            if start_t <= 0.1 and r_num > 1: continue 

            is_lower = (r_num >= 13)
            offset = off_lower if is_lower else off_upper
            adjusted_start = max(0.0, start_t - offset)

            curr_cursor = cursor_l if is_lower else cursor_u
            if adjusted_start < curr_cursor: adjusted_start = curr_cursor # 简单防重叠
            
            text = row.get('medium_text_neutral') or row.get('short_text_neutral')
            if not text or str(text) == 'nan': continue
            
            dur = max(2.5, len(str(text)) * 0.22, float(row.get('span_duration', 0)))
            final_end = adjusted_start + dur
            
            if is_lower: cursor_l = final_end
            else: cursor_u = final_end
            
            schedule.append({
                '回合数': r_num,
                '时间范围': f"{adjusted_start:.1f}-{final_end:.1f}s",
                '解说文本': text
            })
            
        return pd.DataFrame(schedule), len(schedule)//2

    def run(self):
        self.step1_pretreatment()
        merged = self.step3_merge(self.step2_collect_all_modules())
        if merged.empty: 
            print("❌ 无数据")
            return
        
        merged.to_csv(os.path.join(self.cache_dir, "debug_3_merged.csv"), index=False, encoding="utf-8-sig")
        compressed = self.step4_smart_compression(merged)
        final, _ = self.step5_schedule_and_output(compressed)
        
        final.to_csv(os.path.join(self.output_final_dir, "final_schedule.csv"), index=False, encoding='utf-8-sig')
        print(f"🎉 完成！")