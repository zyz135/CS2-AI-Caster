import pandas as pd
import numpy as np
from awpy import Demo
from scipy.spatial.distance import cdist
from config import DEMO_PATH, PREPROCESSED_DATA_PATH
from mapping_table import anchors 
import warnings
import traceback
import time

warnings.filterwarnings("ignore")

# ---------------------------------------------------------
# 1. 3D 矩阵映射
# ---------------------------------------------------------
ANCHOR_COORDS_3D = anchors[['x', 'y', 'z']].values
ANCHOR_NAMES = anchors['name'].values
ANCHOR_MACROS = anchors['macro'].values 

def bulk_mapping_3d_v2(df):
    """同时返回具体点位名和宏观区域"""
    if df.empty: return [], []
    
    # 坐标列名标准化
    rename_map = {}
    for col in ['x', 'y', 'z']:
        if col in df.columns and col.upper() not in df.columns:
            rename_map[col] = col.upper()
    if rename_map: df = df.rename(columns=rename_map)
    
    # 安全检查
    if not {'X', 'Y', 'Z'}.issubset(df.columns):
        return np.full(len(df), "Unknown"), np.full(len(df), "Unknown")

    points_3d = df[['X', 'Y', 'Z']].values
    dists = cdist(points_3d, ANCHOR_COORDS_3D, 'sqeuclidean')
    min_indices = dists.argmin(axis=1)
    
    return ANCHOR_NAMES[min_indices], ANCHOR_MACROS[min_indices]

# ---------------------------------------------------------
# 2. 列名与数据清洗
# ---------------------------------------------------------
def standardize_columns(df):
    if 'side' in df.columns:
        df['side'] = df['side'].astype(str).str.upper().str.strip()
        df['side'] = df['side'].replace({'NONE': '', 'NAN': ''})
    
    if 'health' in df.columns:
        df['health'] = pd.to_numeric(df['health'], errors='coerce').fillna(0)
        
    return df

# ---------------------------------------------------------
# 3. 主处理函数
# ---------------------------------------------------------
def extract_specified_player_data():
    t0 = time.time()
    print(f"🚀 [0.0s] 开始解析 Demo: {DEMO_PATH}")
    
    try:
        # 让 awpy 自动检测 tickrate
        dem = Demo(path=DEMO_PATH, verbose=False)
        dem.parse()
        
        # 获取 tickrate (兼容不同版本)
        demo_tickrate = 128 
        if hasattr(dem, 'tickrate'): demo_tickrate = dem.tickrate
        elif hasattr(dem, 'header') and 'tickrate' in dem.header: demo_tickrate = dem.header['tickrate']
        print(f"ℹ️  检测到 Demo Tickrate: {demo_tickrate}")

        # 处理 ticks 数据 (兼容 Polars/Pandas)
        if hasattr(dem.ticks, "to_pandas"):
            df_ticks = dem.ticks.to_pandas()
        else:
            df_ticks = dem.ticks 
            
        if df_ticks.empty: raise Exception("选手数据为空 (ticks data is empty)")
        
        # 清洗数据
        df_ticks = standardize_columns(df_ticks)
        
        print(f"⏱️ [{time.time()-t0:.2f}s] 解析完成，开始处理...")

        # -------------------------------------------------
        # Step 1: 选手数据处理
        # -------------------------------------------------
        round_start_map = df_ticks.groupby("round_num")["tick"].min().to_dict()
        df_ticks["second"] = (df_ticks["tick"] - df_ticks["round_num"].map(round_start_map)) // demo_tickrate
        
        # === 🟢 [精准修复] C4 安放检测 (基于 dem.bomb) ===
        plant_tick_map = {}
        try:
            df_bomb = pd.DataFrame()
            
            # 1. 读取 dem.bomb 表
            if hasattr(dem, 'bomb'):
                raw = dem.bomb
                if hasattr(raw, "to_pandas"): df_bomb = raw.to_pandas()
                else: df_bomb = pd.DataFrame(raw)
            else:
                print("   ⚠️ 未找到 dem.bomb 属性")

            # 2. 筛选 event == 'plant'
            if not df_bomb.empty:
                if 'event' in df_bomb.columns:
                    # 你的调试信息显示事件类型为 'plant'
                    df_plants = df_bomb[df_bomb['event'] == 'plant']
                    
                    if not df_plants.empty:
                        # 你的调试信息显示包含 'round_num' 和 'tick'
                        plant_tick_map = df_plants.groupby("round_num")["tick"].min().to_dict()
                        print(f"   💣 成功读取 C4 安放事件: 共 {len(plant_tick_map)} 回合")
                    else:
                        print("   ⚠️ dem.bomb 中未发现 'plant' 事件 (本场可能无下包?)")
                else:
                    print("   ❌ 严重: dem.bomb 存在但缺少 'event' 列")
            else:
                # 兜底：尝试从 events 字典找 (旧版兼容)
                if hasattr(dem, 'events') and "bomb_planted" in dem.events:
                    print("   🔄 尝试从 dem.events['bomb_planted'] 读取...")
                    raw = dem.events["bomb_planted"]
                    df_plants = raw.to_pandas() if hasattr(raw, "to_pandas") else pd.DataFrame(raw)
                    if not df_plants.empty:
                        plant_tick_map = df_plants.groupby("round_num")["tick"].min().to_dict()
                        print(f"   💣 成功读取 C4 安放事件 (events): 共 {len(plant_tick_map)} 回合")

        except Exception as e:
            print(f"   ⚠️ 读取安放事件逻辑出错: {e}")
            traceback.print_exc()

        # 应用 C4 状态
        df_ticks['is_c4_planted'] = False
        for r, p_tick in plant_tick_map.items():
            # 标记该回合中，tick 大于等于安装时间的时刻
            mask = (df_ticks['round_num'] == r) & (df_ticks['tick'] >= p_tick)
            df_ticks.loc[mask, 'is_c4_planted'] = True
            
        # 极速采样
        cols_needed = ["round_num", "second", "tick", "name", "side", "X", "Y", "Z", "health", "is_c4_planted"]
        existing_cols = [c for c in cols_needed if c in df_ticks.columns]
        
        df_agg = df_ticks.sort_values("tick").drop_duplicates(
            subset=["round_num", "second", "name"], keep="last"
        )[existing_cols].copy()

        print(f"⏱️ [{time.time()-t0:.2f}s] 映射选手位置 (Name & Macro)...")
        loc_names, loc_macros = bulk_mapping_3d_v2(df_agg)
        df_agg['location_name'] = loc_names
        df_agg['location_macro'] = loc_macros

        # -------------------------------------------------
        # Step 2: 道具处理
        # -------------------------------------------------
        print(f"⏱️ [{time.time()-t0:.2f}s] 提取道具覆盖状态...")
        active_utility_data = []

        def to_pd(obj):
            if hasattr(obj, "to_pandas"): return obj.to_pandas()
            return pd.DataFrame(obj) if obj is not None else pd.DataFrame()

        # 处理 Smoke
        if hasattr(dem, 'smokes'):
            df_smokes = to_pd(dem.smokes)
            if not df_smokes.empty:
                rename_map = {c: c.upper() for c in ['x','y','z'] if c in df_smokes.columns}
                df_smokes = df_smokes.rename(columns=rename_map)
                
                names, _ = bulk_mapping_3d_v2(df_smokes)
                df_smokes['loc'] = names
                
                for row in df_smokes.itertuples():
                    try:
                        r_num = getattr(row, 'round_num', None)
                        if r_num is None: continue
                        s_tick = getattr(row, 'start_tick', getattr(row, 'tick', 0))
                        e_tick = getattr(row, 'end_tick', s_tick + (18 * demo_tickrate))
                        
                        if pd.isna(e_tick): e_tick = s_tick + (18 * demo_tickrate)
                        r_start_tick = round_start_map.get(r_num, s_tick)
                        
                        start_sec = int((s_tick - r_start_tick)//demo_tickrate)
                        end_sec = int((e_tick - r_start_tick)//demo_tickrate)
                        for sec in range(start_sec, end_sec + 1):
                            active_utility_data.append((r_num, sec, f"{row.loc}(Smoke)"))
                    except: continue

        # 处理 Fire
        if hasattr(dem, 'infernos'):
            df_infernos = to_pd(dem.infernos)
            if not df_infernos.empty:
                rename_map = {c: c.upper() for c in ['x','y','z'] if c in df_infernos.columns}
                df_infernos = df_infernos.rename(columns=rename_map)
                
                names, _ = bulk_mapping_3d_v2(df_infernos)
                df_infernos['loc'] = names
                
                for row in df_infernos.itertuples():
                    try:
                        r_num = getattr(row, 'round_num', None)
                        if r_num is None: continue
                        s_tick = getattr(row, 'start_tick', getattr(row, 'tick', 0))
                        e_tick = getattr(row, 'end_tick', s_tick + (7 * demo_tickrate))
                        
                        if pd.isna(e_tick): e_tick = s_tick + (7 * demo_tickrate)
                        r_start_tick = round_start_map.get(r_num, s_tick)
                        
                        start_sec = int((s_tick - r_start_tick)//demo_tickrate)
                        end_sec = int((e_tick - r_start_tick)//demo_tickrate)
                        for sec in range(start_sec, end_sec + 1):
                            active_utility_data.append((r_num, sec, f"{row.loc}(Fire)"))
                    except: continue

        # -------------------------------------------------
        # Step 3: 聚合与保存
        # -------------------------------------------------
        print(f"⏱️ [{time.time()-t0:.2f}s] 合并数据...")
        if active_utility_data:
            df_util = pd.DataFrame(active_utility_data, columns=["round_num", "second", "desc"])
            active_utility_summary = df_util.groupby(["round_num", "second"])["desc"].apply(lambda x: " | ".join(sorted(set(x)))).reset_index().rename(columns={"desc": "active_utility"})
            df_final = pd.merge(df_agg, active_utility_summary, on=["round_num", "second"], how="left")
            df_final["active_utility"] = df_final["active_utility"].fillna("")
        else:
            df_final = df_agg
            df_final["active_utility"] = ""

        # 补齐列
        final_cols = ["round_num", "second", "tick", "name", "side", "location_name", "location_macro", "health", "active_utility", "X", "Y", "Z", "is_c4_planted"]
        for c in final_cols:
            if c not in df_final.columns: df_final[c] = ""
            
        df_final = df_final[final_cols].sort_values(["round_num", "tick", "name"])
        df_final.to_csv(PREPROCESSED_DATA_PATH, index=False, encoding="utf-8-sig")
        
        print(f"✅ [Done] 提取完成！总行数: {len(df_final)}")
        return df_final

    except Exception as e:
        traceback.print_exc()
        print(f"❌ 运行失败: {e}")

def extract_specified_player_data_wrapper(demo_path, output_csv_path):
    global DEMO_PATH, PREPROCESSED_DATA_PATH
    DEMO_PATH = demo_path
    PREPROCESSED_DATA_PATH = output_csv_path
    extract_specified_player_data() 

if __name__ == "__main__":
    extract_specified_player_data()