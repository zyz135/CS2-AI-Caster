import pandas as pd
import numpy as np
from awpy import Demo
from scipy.spatial.distance import cdist
import warnings
import traceback
import os

warnings.filterwarnings("ignore")

# ===================== 坐标映射 =====================
def get_anchors_data():
    try:
        from mapping_table import anchors
        df = anchors.copy()
        df = df.rename(columns={'x': 'X', 'y': 'Y', 'z': 'Z', 'macro': 'area'})
        return df
    except:
        return pd.DataFrame()

def map_coordinates(df_players):
    if df_players.empty: return df_players
    anchors = get_anchors_data()
    if anchors.empty: 
        df_players['area'] = 'Unknown'
        df_players['location'] = 'Unknown'
        return df_players

    player_coords = df_players[['X', 'Y', 'Z']].values
    anchor_coords = anchors[['X', 'Y', 'Z']].values
    
    try:
        dists = cdist(player_coords, anchor_coords)
        min_indices = np.argmin(dists, axis=1)
        df_players['location'] = anchors['name'].values[min_indices]
        df_players['area'] = anchors['area'].values[min_indices]
    except:
        df_players['area'] = 'Unknown'
        
    return df_players

# ===================== 主逻辑 =====================
def extract_specified_player_data_wrapper(demo_path, output_csv_path):
    print(f"🔧 [Pretreatment] 开始处理: {os.path.basename(demo_path)}")
    
    try:
        dem = Demo(demo_path)
        dem.parse() 

        # 🔥 1. 动态获取 Tickrate (128)
        tickrate = dem.tickrate
        print(f"   ℹ️ [Pretreatment] 动态 Tickrate: {tickrate}")

        # 提取数据
        if not hasattr(dem, "ticks") or dem.ticks is None:
             ticks_df = dem.parser.parse_ticks(["X", "Y", "Z", "health", "tick", "round", "player_name", "team_name"])
        else:
             ticks_df = dem.ticks

        if not isinstance(ticks_df, pd.DataFrame):
            ticks_df = ticks_df.to_pandas()

        rename_map = {
            "round": "round_num",
            "player_name": "name",
            "team_name": "side"
        }
        df = ticks_df.rename(columns=rename_map)
        
        # 🔥 2. 降采样：使用 128 tickrate (每秒一行)
        df_sampled = df[df['tick'] % int(tickrate) == 0].copy()
        
        # 获取 Rounds
        rounds_df = dem.rounds
        if not isinstance(rounds_df, pd.DataFrame):
            rounds_df = rounds_df.to_pandas()
            
        df_sampled = df_sampled[df_sampled['round_num'] > 0]

        # 计算相对时间 (Second)
        round_starts = {}
        # 你的 Rounds 数据里有 'freeze_end'，用它更准，或者用 'start'
        # 这里为了稳妥，用 'start' 做基准，但在 Scheduler 里会用 'freeze_end' 做对齐
        if 'start' in rounds_df.columns:
            round_starts = rounds_df.set_index('round_num')['start'].to_dict()
        
        def calc_second(row):
            r = row['round_num']
            t = row['tick']
            start_t = round_starts.get(r, 0)
            if start_t is None: start_t = 0
            # 🔥 3. 准确的秒数计算
            return max(0, (t - start_t) / float(tickrate))

        df_sampled['second'] = df_sampled.apply(calc_second, axis=1)

        # 筛选保存
        keep_cols = ['round_num', 'second', 'tick', 'side', 'name', 'health', 'X', 'Y', 'Z']
        valid_cols = [c for c in keep_cols if c in df_sampled.columns]
        df_final = df_sampled[valid_cols].copy()
        
        if 'side' in df_final.columns:
            df_final['side'] = df_final['side'].astype(str).str.upper()
        
        if 'X' in df_final.columns:
            df_final = map_coordinates(df_final)

        df_final.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
        print(f"✅ [Pretreatment] 预处理完成: {output_csv_path}")
        return df_final

    except Exception as e:
        print(f"❌ [Pretreatment] 失败: {e}")
        traceback.print_exc()
        return pd.DataFrame()