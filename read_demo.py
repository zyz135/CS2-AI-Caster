from awpy import Demo
from pathlib import Path
import csv
import warnings
from mapping_table import mapping_table

warnings.filterwarnings("ignore")

# ===================== 核心配置 =====================
DEMO_PATH = Path("spirit-vs-vitality-m1-mirage.dem")
SMOKE_CSV = "烟雾弹详细信息.csv"
INFERNO_CSV = "燃烧弹详细信息.csv"
OTHER_GRENADE_CSV = "其他投掷物详细信息.csv"


# ===================== 1. 解析 Demo =====================
def parse_demo():
    if not DEMO_PATH.exists():
        raise FileNotFoundError(f"Demo 文件不存在：{DEMO_PATH.absolute()}")

    dem = Demo(DEMO_PATH)
    dem.parse()

    print(f"Demo 解析完成（原始数据量）：")
    print(f"- smokes 专属表：{len(dem.smokes)} 条（聚合后）")
    print(f"- infernos 专属表：{len(dem.infernos)} 条（聚合后）")
    print(f"-grenades 总表：{len(dem.grenades)}条")
    def convert_polars_to_dict(df):
        try:
            return df.to_dicts() if len(df) > 0 else []
        except Exception as e:
            print(f"数据转换警告：{e}")
            return []

    smokes_data = convert_polars_to_dict(dem.smokes)
    infernos_data = convert_polars_to_dict(dem.infernos)
    grenades_row = convert_polars_to_dict(dem.grenades)
    return smokes_data, infernos_data,grenades_row


# ===================== 2. 核心：grenades 去重（保留唯一投掷事件） =====================
def deduplicate_grenades(grenades_data):
    """
    对 grenades 逐帧数据去重：
    - 按「thrower_name + tick + grenade_type + round_num」唯一标识一个投掷事件
    - 只保留每个投掷事件的第一条记录（投掷瞬间）
    """
    entity_latest_data = {}
    projectile={
        "CFlashbangProjectile",
        "CHEGrenadeProjectile"
    }
    for item in grenades_data:
        if not isinstance(item, dict):
            continue  # 跳过非字典数据
        if item["grenade_type"] not in projectile:
            continue
        # 获取当前item的entity_id（兜底：避免KeyError）
        entity_id = item.get("entity_id", None)
        if entity_id is None or entity_id == "":
            continue  # 无有效entity_id的item跳过（或按需处理）

        # 关键逻辑：直接覆盖 → 最后一次遍历到的item会保留
        entity_latest_data[entity_id] = item

    # 提取最终去重结果（仅保留每个entity_id的最后一条数据）
    deduplicated = list(entity_latest_data.values())


    print(f"grenades 去重完成：{len(grenades_data)} 条 → {len(deduplicated)} 条（匹配实际投掷次数）")
    return deduplicated


# ===================== 3. 数据处理函数（复用之前逻辑） =====================
def process_smoke_inferno_data(data, table_type):
    processed_data = []
    FIELD_MAP = {
        "entity_id": "entity_id",
        "thrower_name": "thrower_name",
        "thrower_side": "thrower_side",
        "X": "X",
        "Y": "Y",
        "Z": "Z",
        "duration": "duration",
        "tick": "start_tick",
        "round_num": "round_num"
    }

    for item in data:
        if not isinstance(item, dict):
            continue
        entity_id = item.get("entity_id", None)
        thrower = item.get(FIELD_MAP["thrower_name"], "未知选手")
        thrower_side = item.get(FIELD_MAP["thrower_side"], "未知阵营/队伍")
        land_x = item.get(FIELD_MAP["X"], 0.0)
        land_y = item.get(FIELD_MAP["Y"], 0.0)
        land_z = item.get(FIELD_MAP["Z"], 0.0)
        land_coords = f"({land_x:.1f},{land_y:.1f},{land_z:.1f})"

        try:
            land_area = mapping_table(land_x, land_y, land_z)
        except Exception as e:
            print(f"区域映射警告：{e}")
            land_area = "未知区域"

        raw_duration = item.get(FIELD_MAP["duration"], 0.0)
        final_duration = round(raw_duration, 1) if raw_duration > 0 else (18.0 if table_type == "smoke" else 7.0)
        grenade_type = "smoke" if table_type == "smoke" else "inferno"
        tick = item.get(FIELD_MAP["tick"], 0)
        round_num = item.get(FIELD_MAP["round_num"], 0)

        row = {
            "entity_id": entity_id,
            "投掷人": thrower,
            "投掷人所在队伍/阵营": thrower_side,
            "落点坐标(X,Y,Z)": land_coords,
            "落点所在范围": land_area,
            "投掷物类型": grenade_type,
            "烟雾弹持续时间(秒)": final_duration if table_type == "smoke" else "",
            "燃烧瓶燃烧时间(秒)": final_duration if table_type == "inferno" else "",
            "tick时间戳": tick,
            "回合数": round_num
        }
        processed_data.append(row)

    processed_data.sort(key=lambda x: int(x["tick时间戳"]))
    return processed_data


def process_grenades_total_data(grenades_data):
    processed_data = []
    FIELD_MAP = {
        "entity_id": "entity_id",
        "thrower": "thrower",
        "grenade_type": "grenade_type",
        "end_X": "X",
        "end_Y": "Y",
        "end_Z": "Z",
        "tick": "tick",
        "round_num": "round_num"
    }

    for item in grenades_data:
        if not isinstance(item, dict):
            continue

        entity_id = item.get("entity_id", None)
        thrower = item.get(FIELD_MAP["thrower"])
        grenade_type = item.get(FIELD_MAP["grenade_type"], "未知投掷物")
        land_x = item.get(FIELD_MAP["end_X"], 0.0)
        land_y = item.get(FIELD_MAP["end_Y"], 0.0)
        land_z = item.get(FIELD_MAP["end_Z"], 0.0)
        land_coords = f"({land_x:.1f},{land_y:.1f},{land_z:.1f})"

        try:
            land_area = mapping_table(land_x, land_y, land_z)
        except:
            land_area = "未知区域"

        tick = item.get(FIELD_MAP["tick"], 0)
        round_num = item.get(FIELD_MAP["round_num"], 0)

        row = {
            "entity_id": entity_id,
            "投掷人": thrower,
            "落点坐标(X,Y,Z)": land_coords,
            "落点所在范围": land_area,
            "投掷物类型": grenade_type,
            "tick时间戳": tick,
            "回合数": round_num
        }
        processed_data.append(row)

    processed_data.sort(key=lambda x: int(x["tick时间戳"]))
    return processed_data


# ===================== 4. 生成 CSV =====================
def write_csv(file_path, data):
    csv_header = [
        "entity_id","投掷人", "投掷人所在队伍/阵营", "落点坐标(X,Y,Z)", "落点所在范围",
        "投掷物类型", "烟雾弹持续时间(秒)", "燃烧瓶燃烧时间(秒)", "tick时间戳", "回合数"
    ]
    with open(file_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_header, restval="")
        writer.writeheader()
        writer.writerows(data)
    print(f"✅ {file_path} 已生成，共 {len(data)} 条数据")


# ===================== 主流程（新增去重） =====================
def makeCSV():
    try:
        smokes_raw, infernos_raw,grenades_row = parse_demo()

        # 关键步骤：对 grenades 逐帧数据去重
        other_grenades_raw=deduplicate_grenades(grenades_row)

        # 处理数据
        smoke_processed = process_smoke_inferno_data(smokes_raw, "smoke")
        inferno_processed = process_smoke_inferno_data(infernos_raw, "inferno")
        other_grenades_processed = process_grenades_total_data(other_grenades_raw)

        # 生成文件
        write_csv(SMOKE_CSV, smoke_processed)
        write_csv(INFERNO_CSV, inferno_processed)
        write_csv(OTHER_GRENADE_CSV, other_grenades_processed)

        # 汇总
        print("\n=== 最终数据量汇总 ===")
        print(f"1. 烟雾弹（专属表）：{len(smoke_processed)} 条")
        print(f"2. 燃烧弹（专属表）：{len(inferno_processed)} 条")
        print(f"3.其他投掷物：{len(other_grenades_processed)} 条")

    except Exception as e:
        print(f"执行错误：{e}")
        import traceback

        traceback.print_exc()