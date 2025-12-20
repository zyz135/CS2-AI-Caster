import pandas as pd
import numpy as np
from awpy import Demo
from scipy.spatial.distance import cdist
from config import DEMO_PATH, TICKRATE, PREPROCESSED_DATA_PATH
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
    
    # 你的数据里已经是大写 X, Y, Z 了，但为了保险起见保留映射
    rename_map = {}
    if 'x' in df.columns and 'X' not in df.columns: rename_map['x'] = 'X'
    if 'y' in df.columns and 'Y' not in df.columns: rename_map['y'] = 'Y'
    if 'z' in df.columns and 'Z' not in df.columns: rename_map['z'] = 'Z'
    if rename_map: df = df.rename(columns=rename_map)
    
    # 安全检查：如果没有坐标列，返回 Unknown
    if not {'X', 'Y', 'Z'}.issubset(df.columns):
        return np.full(len(df), "Unknown"), np.full(len(df), "Unknown")

    points_3d = df[['X', 'Y', 'Z']].values
    dists = cdist(points_3d, ANCHOR_COORDS_3D, 'sqeuclidean')
    min_indices = dists.argmin(axis=1)
    
    return ANCHOR_NAMES[min_indices], ANCHOR_MACROS[min_indices]

# ---------------------------------------------------------
# 2. 列名与数据清洗 (核心修复)
# ---------------------------------------------------------
def standardize_columns(df):
    """
    针对你的数据结构 ['health', 'place', 'side', 'X', 'Y', 'Z', 'tick', 'name', 'round_num'] 进行清洗
    """
    # 1. 确保 side 列存在并标准化
    if 'side' in df.columns:
        # 转字符串 -> 转大写 -> 去空格
        df['side'] = df['side'].astype(str).str.upper().str.strip()
        
        # 处理 None 或 'NAN'
        df['side'] = df['side'].replace({'NONE': '', 'NAN': ''})
    
    # 2. 确保 health 是数字
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
        dem = Demo(path=DEMO_PATH, tickrate=TICKRATE, verbose=False)
        dem.parse()
        
        df_ticks = dem.ticks.to_pandas()
        if df_ticks.empty: raise Exception("选手数据为空")
        
        # === 🟢 修复步骤：清洗数据 ===
        df_ticks = standardize_columns(df_ticks)
        # ==========================
        
        print(f"⏱️ [{time.time()-t0:.2f}s] 解析完成，开始处理...")

        # -------------------------------------------------
        # Step 1: 选手数据处理
        # -------------------------------------------------
        round_start_map = df_ticks.groupby("round_num")["tick"].min().to_dict()
        df_ticks["second"] = (df_ticks["tick"] - df_ticks["round_num"].map(round_start_map)) // TICKRATE
        
        # === C4 安放检测 (基于事件，不依赖 inventory) ===
        plant_tick_map = {}
        try:
            # 兼容不同 awpy 版本读取事件的方式
            if hasattr(dem, 'bomb_planted'): 
                df_plants = dem.bomb_planted.to_pandas()
            else: 
                df_plants = dem.events.get("bomb_planted", pd.DataFrame())
                
            if not df_plants.empty:
                plant_tick_map = df_plants.groupby("round_num")["tick"].min().to_dict()
                print(f"   💣 成功读取 C4 安放事件: 共 {len(plant_tick_map)} 回合")
        except Exception as e:
            print(f"   ⚠️ 读取安放事件失败 (非致命): {e}")

        df_ticks['is_c4_planted'] = False
        for r, p_tick in plant_tick_map.items():
            mask = (df_ticks['round_num'] == r) & (df_ticks['tick'] >= p_tick)
            df_ticks.loc[mask, 'is_c4_planted'] = True
            
        # 极速采样 (去掉 has_c4，因为没有 inventory 列)
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

        # 处理 Smoke
        if hasattr(dem, 'smokes'):
            df_smokes = dem.smokes.to_pandas()
            if not df_smokes.empty:
                # 简单列名适配
                if 'x' in df_smokes.columns: df_smokes = df_smokes.rename(columns={'x':'X', 'y':'Y', 'z':'Z'})
                
                names, _ = bulk_mapping_3d_v2(df_smokes)
                df_smokes['loc'] = names
                for row in df_smokes.itertuples():
                    r_num = getattr(row, 'round_num', None)
                    if r_num is None: continue
                    s_tick, e_tick = row.start_tick, row.end_tick
                    if pd.isna(e_tick): e_tick = s_tick + (18 * TICKRATE)
                    r_start_tick = round_start_map.get(r_num, s_tick)
                    if r_start_tick is None: continue
                    
                    start_sec = int((s_tick - r_start_tick)//TICKRATE)
                    end_sec = int((e_tick - r_start_tick)//TICKRATE)
                    for sec in range(start_sec, end_sec + 1):
                        active_utility_data.append((r_num, sec, f"{row.loc}(Smoke)"))

        # 处理 Fire
        if hasattr(dem, 'infernos'):
            df_infernos = dem.infernos.to_pandas()
            if not df_infernos.empty:
                if 'x' in df_infernos.columns: df_infernos = df_infernos.rename(columns={'x':'X', 'y':'Y', 'z':'Z'})
                
                names, _ = bulk_mapping_3d_v2(df_infernos)
                df_infernos['loc'] = names
                for row in df_infernos.itertuples():
                    r_num = getattr(row, 'round_num', None)
                    if r_num is None: continue
                    s_tick, e_tick = row.start_tick, row.end_tick
                    if pd.isna(e_tick): e_tick = s_tick + (7 * TICKRATE)
                    r_start_tick = round_start_map.get(r_num, s_tick)
                    if r_start_tick is None: continue
                    
                    start_sec = int((s_tick - r_start_tick)//TICKRATE)
                    end_sec = int((e_tick - r_start_tick)//TICKRATE)
                    for sec in range(start_sec, end_sec + 1):
                        active_utility_data.append((r_num, sec, f"{row.loc}(Fire)"))

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

        # 确保 location_macro 存在
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