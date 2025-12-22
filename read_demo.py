from awpy import Demo
from pathlib import Path
import csv
import warnings
from mapping_table import mapping_table

warnings.filterwarnings("ignore")

# ===================== 配置 (仅保留输出文件名) =====================
SMOKE_CSV = "烟雾弹详细信息.csv"
INFERNO_CSV = "燃烧弹详细信息.csv"
OTHER_GRENADE_CSV = "其他投掷物详细信息.csv"

# ===================== 1. 解析 Demo (接收参数) =====================
def parse_demo(demo_path_input):
    """
    解析 Demo 文件，不再读取全局变量，而是读取传入的 demo_path_input
    """
    demo_path = Path(demo_path_input)
    
    if not demo_path.exists():
        raise FileNotFoundError(f"Demo 文件不存在：{demo_path.absolute()}")

    print(f"🔧 [read_demo] 正在解析: {demo_path.name}")
    dem = Demo(str(demo_path))
    dem.parse()

    def convert_polars_to_dict(df):
        try:
            return df.to_dicts() if len(df) > 0 else []
        except Exception as e:
            if isinstance(df, list): return df
            return []

    smokes_data = convert_polars_to_dict(dem.smokes) if hasattr(dem, 'smokes') else []
    infernos_data = convert_polars_to_dict(dem.infernos) if hasattr(dem, 'infernos') else []
    grenades_row = convert_polars_to_dict(dem.grenades) if hasattr(dem, 'grenades') else []

    return smokes_data, infernos_data, grenades_row


# ===================== 2. 去重逻辑 =====================
def deduplicate_grenades(grenades_data):
    entity_latest_data = {}
    projectile={"CFlashbangProjectile", "CHEGrenadeProjectile"}
    for item in grenades_data:
        if not isinstance(item, dict): continue 
        if item.get("grenade_type") not in projectile: continue
        entity_id = item.get("entity_id", None)
        if entity_id is None: continue
        entity_latest_data[entity_id] = item
    return list(entity_latest_data.values())


# ===================== 3. 数据处理 =====================
def process_smoke_inferno_data(data, table_type):
    processed_data = []
    for item in data:
        item_lower = {k.lower(): v for k, v in item.items()} if isinstance(item, dict) else {}
        def get_val(key_list, default=None):
            for k in key_list:
                if k in item: return item[k]
                if k.lower() in item_lower: return item_lower[k.lower()]
            return default

        entity_id = get_val(["entity_id"])
        thrower = get_val(["thrower_name", "thrower"], "未知选手")
        thrower_side = get_val(["thrower_side", "team_name", "side"], "未知")
        land_x = get_val(["X", "x"], 0.0)
        land_y = get_val(["Y", "y"], 0.0)
        land_z = get_val(["Z", "z"], 0.0)
        
        land_coords = f"({land_x:.1f},{land_y:.1f},{land_z:.1f})"
        try: land_area = mapping_table(land_x, land_y, land_z)
        except: land_area = "未知区域"

        raw_duration = get_val(["duration"], 0.0)
        final_duration = round(raw_duration, 1) if raw_duration > 0 else (18.0 if table_type == "smoke" else 7.0)
        grenade_type = "smoke" if table_type == "smoke" else "inferno"
        tick = get_val(["start_tick", "tick"], 0)
        round_num = get_val(["round_num", "round"], 0)

        processed_data.append({
            "entity_id": entity_id, "投掷人": thrower, "投掷人所在队伍/阵营": thrower_side,
            "落点坐标(X,Y,Z)": land_coords, "落点所在范围": land_area, "投掷物类型": grenade_type,
            "烟雾弹持续时间(秒)": final_duration if table_type == "smoke" else "",
            "燃烧瓶燃烧时间(秒)": final_duration if table_type == "inferno" else "",
            "tick时间戳": tick, "回合数": round_num
        })
    processed_data.sort(key=lambda x: int(x["tick时间戳"]))
    return processed_data


def process_grenades_total_data(grenades_data):
    processed_data = []
    for item in grenades_data:
        def get_val(keys, default):
            for k in keys:
                if k in item: return item[k]
            return default
        entity_id = get_val(["entity_id"], None)
        thrower = get_val(["thrower", "thrower_name"], "未知")
        grenade_type = get_val(["grenade_type"], "未知")
        land_x = get_val(["end_X", "x", "X"], 0.0)
        land_y = get_val(["end_Y", "y", "Y"], 0.0)
        land_z = get_val(["end_Z", "z", "Z"], 0.0)
        land_coords = f"({land_x:.1f},{land_y:.1f},{land_z:.1f})"
        try: land_area = mapping_table(land_x, land_y, land_z)
        except: land_area = "未知区域"
        tick = get_val(["tick"], 0)
        round_num = get_val(["round_num"], 0)
        processed_data.append({
            "entity_id": entity_id, "投掷人": thrower, "落点坐标(X,Y,Z)": land_coords,
            "落点所在范围": land_area, "投掷物类型": grenade_type, "tick时间戳": tick, "回合数": round_num
        })
    processed_data.sort(key=lambda x: int(x["tick时间戳"]))
    return processed_data


# ===================== 4. 生成 CSV (被 createTexts 调用) =====================
def write_csv(file_path, data):
    if not data: return
    csv_header = list(data[0].keys())
    with open(file_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_header, restval="")
        writer.writeheader()
        writer.writerows(data)

def makeCSV(target_demo_path):
    try:
        smokes_raw, infernos_raw, grenades_row = parse_demo(target_demo_path)
        other_grenades_raw = deduplicate_grenades(grenades_row)
        smoke_processed = process_smoke_inferno_data(smokes_raw, "smoke")
        inferno_processed = process_smoke_inferno_data(infernos_raw, "inferno")
        other_grenades_processed = process_grenades_total_data(other_grenades_raw)
        write_csv(SMOKE_CSV, smoke_processed)
        write_csv(INFERNO_CSV, inferno_processed)
        write_csv(OTHER_GRENADE_CSV, other_grenades_processed)
    except Exception as e:
        print(f"❌ makeCSV 执行错误：{e}")