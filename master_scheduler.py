import pandas as pd
import numpy as np
import os
import sys

# ==========================================
# 1. 全局变量预先初始化
# ==========================================
run_tactical_analysis = None
set_tactical_api = None
run_grenade_analysis = None
set_grenade_api = None
process_dem_file = None
get_eco_df = None
extract_specified_player_data_wrapper = None

print("📦 [System] 正在加载模块...")

# ==========================================
# 2. 安全导入子模块
# ==========================================
try:
    from data_analysis import run_tactical_analysis, setAPI as set_tactical_api
except Exception as e: print(f"   ⚠️ 警告: 无法加载战术模块: {e}")

try:
    from createTexts import run_grenade_analysis, setAPI_KEY as set_grenade_api
except Exception as e: print(f"   ⚠️ 警告: 无法加载道具模块: {e}")

try:
    from final_kill import process_dem_file
except Exception as e: print(f"   ⚠️ 警告: 无法加载击杀模块: {e}")

try:
    from eco_and_round import get_events_df as get_eco_df
except Exception as e: print(f"   ⚠️ 警告: 无法加载经济模块: {e}")

try:
    from pretreatment import extract_specified_player_data_wrapper
except Exception as e: print(f"   ⚠️ 警告: 无法加载预处理模块: {e}")


# ==========================================
# 3. 调度器类定义
# ==========================================
class MasterScheduler:
    def __init__(self, demo_path, api_key):
        self.demo_path = demo_path
        self.api_key = api_key
        
        self.base_name = os.path.splitext(os.path.basename(demo_path))[0]
        self.output_dir = os.path.join("data", self.base_name)
        os.makedirs(self.output_dir, exist_ok=True)
        self.raw_data_path = os.path.join(self.output_dir, "1_raw_data.csv")
        
        self.round_global_offsets = {} 
        self.round_start_ticks = {}
        
        self._distribute_api_key()

    def _distribute_api_key(self):
        print(f"🔑 [System] 正在分发 API Key...")
        if set_tactical_api: set_tactical_api(self.api_key)
        if set_grenade_api: set_grenade_api(self.api_key)
        os.environ["DASHSCOPE_API_KEY"] = self.api_key
        os.environ["OPENAI_API_KEY"] = self.api_key

    def _calculate_offsets(self):
        if not os.path.exists(self.raw_data_path): return
        try:
            df = pd.read_csv(self.raw_data_path, usecols=['round_num', 'second', 'tick'])
            self.round_start_ticks = df.groupby('round_num')['tick'].min().to_dict()
            rounds = sorted(df['round_num'].unique())
            running_time = 0.0
            is_second_half = False
            for r_num in rounds:
                if r_num == 13 and not is_second_half:
                    running_time = 0.0
                    is_second_half = True
                self.round_global_offsets[r_num] = running_time
                max_sec = df[df['round_num'] == r_num]['second'].max()
                running_time += max_sec
            print(f"✅ [System] 已建立战术时间基准 (覆盖 {len(rounds)} 个回合)")
        except Exception as e:
            print(f"⚠️ 无法计算时间偏移: {e}")

    def step1_pretreatment(self):
        if os.path.exists(self.raw_data_path):
            print("✅ [Step 1] 基础数据已存在，跳过提取。")
            self._calculate_offsets()
            return True

        if not extract_specified_player_data_wrapper:
            print("❌ 错误：缺少预处理模块且无缓存数据。")
            return False

        print("🔄 [Step 1] 提取基础数据...")
        try:
            extract_specified_player_data_wrapper(self.demo_path, self.raw_data_path)
            self._calculate_offsets()
            return True
        except Exception as e:
            print(f"❌ 预处理执行失败: {e}")
            return False

    def _align_to_tactical_standard(self, df, module_type):
        if df.empty: return df
        
        # 1. 战术模块: 已经是净时间，无需转换
        if module_type == 'tactical': return df
        
        # 2. 击杀模块: 已经是相对时间 (Relative)，只需加 Offset
        if module_type == 'kill':
            def fix_relative(row):
                r = row['round_num']
                rel_t = row['start_time']
                offset = self.round_global_offsets.get(r, 0.0)
                return offset + rel_t
            df['start_time'] = df.apply(fix_relative, axis=1)
            return df
            
        # 3. 道具 & 经济: 是绝对时间 (Absolute)，必须先减去 RoundStart，再加 Offset
        if module_type in ['grenade', 'economy']:
            def fix_absolute(row):
                r = row['round_num']
                abs_t_sec = row['start_time'] # 这里是绝对时间 (秒)
                
                # 获取该回合的绝对开始时间 (秒)
                start_tick = self.round_start_ticks.get(r)
                rel_t = 0.0
                if start_tick:
                    base_sec = start_tick / 128.0
                    # 计算相对时间 (去掉了热身/暂停)
                    rel_t = max(0.0, abs_t_sec - base_sec)
                
                # 加上全局偏移，对齐到战术时间轴
                offset = self.round_global_offsets.get(r, 0.0)
                return offset + rel_t

            df['start_time'] = df.apply(fix_absolute, axis=1)
            return df
            
        return df

    def step2_collect_all_modules(self):
        all_dfs = []
        print("\n🚀 [Step 2] 并行调用子模块...")

        # A. 战术
        t1 = os.path.join(self.output_dir, "tactical_gen_cache.csv")
        df_tact = pd.DataFrame()
        if os.path.exists(t1):
            print(f"   >>> 战术模块: 读取缓存")
            df_tact = pd.read_csv(t1)
        elif run_tactical_analysis:
            print(f"   >>> 战术模块: 运行生成...")
            try: df_tact = run_tactical_analysis(self.raw_data_path, self.output_dir)
            except: pass
        if not df_tact.empty:
            df_tact['module'] = 'tactical'
            all_dfs.append(df_tact)

        # B. 击杀
        k1 = os.path.join(self.output_dir, "kill_gen_cache.csv")
        if os.path.exists(k1):
            print("   >>> 击杀模块: 读取缓存")
            df = pd.read_csv(k1)
            df['module'] = 'kill'
            df = self._align_to_tactical_standard(df, 'kill')
            all_dfs.append(df)
        elif process_dem_file:
            print("   >>> 击杀模块: 运行生成...")
            try:
                df = process_dem_file(self.demo_path, self.api_key, verbose=False)
                if not df.empty:
                    df['module'] = 'kill'
                    df = self._align_to_tactical_standard(df, 'kill')
                    all_dfs.append(df)
            except Exception as e: print(f"Error Kill: {e}")

        # C. 道具
        g1 = os.path.join(self.output_dir, "grenade_gen_cache.csv")
        if os.path.exists(g1):
            print("   >>> 道具模块: 读取缓存")
            df = pd.read_csv(g1)
            df['module'] = 'grenade'
            df = self._align_to_tactical_standard(df, 'grenade')
            all_dfs.append(df)
        elif run_grenade_analysis:
            print("   >>> 道具模块: 运行生成...")
            try:
                df = run_grenade_analysis(self.demo_path)
                if not df.empty:
                    df['module'] = 'grenade'
                    df = self._align_to_tactical_standard(df, 'grenade')
                    all_dfs.append(df)
            except: pass

        # D. 经济
        e1 = os.path.join(self.output_dir, "economy_gen_cache.csv")
        if os.path.exists(e1):
            print("   >>> 经济模块: 读取缓存")
            df = pd.read_csv(e1)
            df['module'] = 'economy'
            df = self._align_to_tactical_standard(df, 'economy') 
            all_dfs.append(df)
        elif get_eco_df:
            print("   >>> 经济模块: 运行生成...")
            try:
                df = get_eco_df(self.demo_path, enable_llm=True)
                if not df.empty:
                    df['module'] = 'economy'
                    df = self._align_to_tactical_standard(df, 'economy')
                    all_dfs.append(df)
            except: pass

        return all_dfs

    def step3_merge(self, all_dfs):
        if not all_dfs: return pd.DataFrame()
        
        core_cols = ['event_id', 'round_num', 'start_time', 'priority', 
                     'short_text_neutral', 'medium_text_neutral', 'long_text_neutral', 'module']
        
        cleaned = []
        for df in all_dfs:
            temp = df.copy()
            for c in core_cols:
                if c not in temp.columns: temp[c] = ""
            cleaned.append(temp[core_cols])
            
        final_df = pd.concat(cleaned, ignore_index=True)
        final_df['start_time'] = pd.to_numeric(final_df['start_time'], errors='coerce').fillna(0)
        final_df['priority'] = pd.to_numeric(final_df['priority'], errors='coerce').fillna(1)
        final_df['round_num'] = pd.to_numeric(final_df['round_num'], errors='coerce').fillna(0)

        # 经济解说优先级提升
        mask_eco = final_df['module'] == 'economy'
        final_df.loc[mask_eco, 'priority'] += 8 

        return final_df.sort_values(by=['round_num', 'start_time', 'priority'], ascending=[True, True, False])

    def step5_schedule_and_output(self, df):
        print("⚔️ [Step 4] 智能排期 v3.1 (中长文本优先 + 强制插队)...")
        schedule = []
        global_cursor = 0.0
        
        half_break_index = None
        is_second_half_started = False
        
        df = df.sort_values(by=['round_num', 'start_time', 'priority'], ascending=[True, True, False])
        
        for _, row in df.iterrows():
            r_num = row['round_num']
            start_t = row['start_time']
            prio = row['priority']
            module = row['module']
            
            # === 🔥 修改点：只取 Medium 或 Long ===
            text = str(row.get('medium_text_neutral', '')).strip()
            if not text or text.lower() in ['nan', 'none', '']:
                text = str(row.get('long_text_neutral', '')).strip()
            
            if not text or text.lower() in ['nan', 'none', '']: 
                continue
            # =======================================

            if r_num >= 13 and not is_second_half_started:
                is_second_half_started = True
                half_break_index = len(schedule)
                if start_t < global_cursor: global_cursor = 0.0

            text = text.replace("短版", "").replace("中版", "").replace("长版", "").replace("---", "").strip()
            if not text: continue

            # 动态时长
            est_duration = len(text) / 5.0
            dur = max(2.5, min(est_duration, 10.0)) 
            if module == 'grenade': dur = min(dur, 3.0) 

            # 🚀 强制插队逻辑
            final_start = start_t
            
            if module == 'tactical':
                if start_t > global_cursor + 8.0:
                    final_start = start_t 
                else:
                    if global_cursor - final_start < 25.0: 
                        final_start = global_cursor
                    else:
                        continue 
            else:
                if final_start < global_cursor:
                     if prio >= 6: 
                         final_start = global_cursor
                     else:
                         if global_cursor - final_start < 3.0:
                             final_start = global_cursor
                         else:
                             continue
            
            final_end = final_start + dur
            schedule.append({
                '时间范围': f"{final_start:.1f}-{final_end:.1f}s",
                '解说文本': text
            })
            global_cursor = final_end
            
        if half_break_index is None: half_break_index = len(schedule)
        return pd.DataFrame(schedule), half_break_index

    def run(self):
        if not self.step1_pretreatment(): return

        all_dfs = self.step2_collect_all_modules()
        merged = self.step3_merge(all_dfs)
        
        if merged.empty:
            print("❌ 错误：无数据生成")
            return
            
        final_sch, split_idx = self.step5_schedule_and_output(merged)
        
        sch1 = final_sch.iloc[:split_idx]
        sch2 = final_sch.iloc[split_idx:]
        
        p1 = os.path.join(self.output_dir, "final_upper_half.csv")
        p2 = os.path.join(self.output_dir, "final_lower_half.csv")
        
        sch1.to_csv(p1, index=False, encoding="utf-8-sig")
        sch2.to_csv(p2, index=False, encoding="utf-8-sig")
        
        print(f"\n✅ 全部完成! \n上半场: {p1} ({len(sch1)}条)\n下半场: {p2} ({len(sch2)}条)")