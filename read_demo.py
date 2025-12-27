from awpy import Demo
from pathlib import Path
import csv
import warnings
import pandas as pd
from mapping_table import mapping_table
import config  # 引入配置

warnings.filterwarnings("ignore")

SMOKE_CSV = "烟雾弹详细信息.csv"
INFERNO_CSV = "燃烧弹详细信息.csv"

def parse_demo(demo_path_input):
    demo_path = Path(demo_path_input)
    print(f"🔧 [read_demo] 解析: {demo_path.name}")
    
    dem = Demo(str(demo_path))
    dem.parse()

    # 🔥🔥🔥 强制使用配置中的 64 🔥🔥🔥
    tickrate = config.TICKRATE 
    print(f"   ℹ️ [read_demo] 强制 Tickrate: {tickrate}")

    def to_df(data):
        if hasattr(data, "to_pandas"): return data.to_pandas()
        return pd.DataFrame(data)

    smokes = to_df(dem.smokes).to_dict('records') if hasattr(dem, 'smokes') else []
    infernos = to_df(dem.infernos).to_dict('records') if hasattr(dem, 'infernos') else []
    
    return smokes, infernos, tickrate

def process_grenade_data(raw_data, g_type_name, tickrate):
    processed = []
    
    for item in raw_data:
        thrower = item.get("thrower_name", "Unknown")
        entity_id = item.get("entity_id", 0)
        land_x = item.get("X", 0)
        land_y = item.get("Y", 0)
        land_z = item.get("Z", 0)
        
        try:
            land_coords = f"({land_x:.1f}, {land_y:.1f}, {land_z:.1f})"
            land_area = mapping_table(land_x, land_y, land_z)
        except: 
            land_area = "未知区域"

        # 优先读取 start_tick
        tick = item.get("start_tick", 0)
        if tick == 0: tick = item.get("tick", 0)

        round_num = item.get("round_num", 0)

        processed.append({
            "entity_id": entity_id, 
            "投掷人": thrower, 
            "落点坐标(X,Y,Z)": land_coords,
            "落点所在范围": land_area, 
            "投掷物类型": g_type_name, 
            "tick时间戳": tick, 
            # 🔥 统一时间基准
            "start_time": tick / float(tickrate),
            "回合数": round_num
        })
    
    processed.sort(key=lambda x: int(x["tick时间戳"]))
    return processed

def write_csv(file_path, data):
    if not data: return
    with open(file_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)

def makeCSV(target_demo_path):
    smokes_raw, infernos_raw, tickrate = parse_demo(target_demo_path)
    
    s_proc = process_grenade_data(smokes_raw, "Smoke (烟雾弹)", tickrate)
    i_proc = process_grenade_data(infernos_raw, "Incendiary (燃烧弹)", tickrate)
    
    write_csv(SMOKE_CSV, s_proc)
    write_csv(INFERNO_CSV, i_proc)
    
    print(f"✅ [read_demo] 道具解析完成")