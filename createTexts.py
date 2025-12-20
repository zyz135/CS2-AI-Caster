import pandas as pd
import json
from pathlib import Path
from openai import OpenAI
import time
import os
from dotenv import load_dotenv
import read_demo
from read_demo import makeCSV

OUTPUT_CSV="解说文本.csv"

#生成需要的CSV
makeCSV()


# ===================== 核心配置（仅读烟雾弹+燃烧弹专属CSV） =====================
CSV_PATHS = {
    "smoke": "烟雾弹详细信息.csv",  # 烟雾弹专属CSV
    "inferno": "燃烧弹详细信息.csv",  # 燃烧弹专属CSV
    "other": "其他投掷物详细信息.csv"
}

OPENAI_API_KEY = None
def setAPI_KEY(api_key):
    global OPENAI_API_KEY
    if api_key is not None:
        OPENAI_API_KEY = api_key
    else:
        OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # 读环境变量

setAPI_KEY(None)
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
MODEL_NAME = "qwen3-max"



# ===================== 1. 读取烟雾弹+燃烧弹专属CSV并合并 =====================
def load_grenades_data():
    """分别读取烟雾弹、燃烧弹CSV，合并后去重排序"""
    df_combined = pd.DataFrame()

    for grenade_type, csv_path in CSV_PATHS.items():
        if not Path(csv_path).exists():
            raise FileNotFoundError(f"专属CSV文件不存在：{csv_path}")

        # 读取单类CSV，提取核心字段并清洗
        df = pd.read_csv(csv_path, encoding="utf-8-sig",dtype={"entity_id": int})
        core_cols = ["entity_id","投掷人", "投掷人所在队伍/阵营", "落点所在范围", "投掷物类型", "tick时间戳","回合数"]
        df = df[core_cols].dropna(subset=["投掷人", "落点所在范围", "tick时间戳"])

        # 合并到总DataFrame
        df_combined = pd.concat([df_combined, df], ignore_index=True)
        print(f"✅ 读取 {csv_path}: {len(df)} 条{'烟雾弹' if grenade_type == 'smoke' else '燃烧弹'}数据")

    # 按tick排序
    df_combined = df_combined.sort_values(by="tick时间戳", key=lambda x: x.astype(int))  # 确保tick是数字排序

    merged_data = []
    if len(df_combined) == 0:
        print("\n📊 合并后总数据：0 条")
        return []

    # 初始化第一个分组
    current_group = {
        "tick": df_combined.iloc[0]["tick时间戳"],
        "type": df_combined.iloc[0]["投掷物类型"],
        "throwers": [df_combined.iloc[0]["投掷人"]],
        "areas": [df_combined.iloc[0]["落点所在范围"]],
        "team": df_combined.iloc[0]["投掷人所在队伍/阵营"],
        "round_num": df_combined.iloc[0]["回合数"],
        "entity_id": df_combined.iloc[0]["entity_id"]
    }

    # 遍历剩余数据，按条件分组
    for idx in range(1, len(df_combined)):
        row = df_combined.iloc[idx]
        tick_diff = row["tick时间戳"] - current_group["tick"]
        same_type = (row["投掷物类型"] == current_group["type"])

        # 条件：tick差<3*128 且 类型相同 → 加入当前分组
        if tick_diff < 3 * 128 and same_type:
            current_group["throwers"].append(row["投掷人"])
            current_group["areas"].append(row["落点所在范围"])
        else:
            # 条件不满足 → 合并当前分组，加入结果
            merged_row = {
                "entity_id": current_group["entity_id"],
                "投掷人": "多个" if len(current_group["throwers"]) > 1 else current_group["throwers"][0],
                "投掷人所在队伍/阵营": current_group["team"],
                "落点所在范围": "、".join(current_group["areas"]),  # 合并落点范围
                "投掷物类型": current_group["type"],
                "tick时间戳": current_group["tick"],  # 保留最早的tick
                "回合数": current_group["round_num"]
            }
            merged_data.append(merged_row)
            # 初始化新分组
            current_group = {
                "tick": row["tick时间戳"],
                "type": row["投掷物类型"],
                "throwers": [row["投掷人"]],
                "areas": [row["落点所在范围"]],
                "team": row["投掷人所在队伍/阵营"],
                "round_num": row["回合数"],
                "entity_id": row["entity_id"]
            }

    # 处理最后一个分组
    merged_row = {
        "entity_id": current_group["entity_id"],
        "投掷人": "多个" if len(current_group["throwers"]) > 1 else current_group["throwers"][0],
        "投掷人所在队伍/阵营": current_group["team"],
        "落点所在范围": "、".join(current_group["areas"]),
        "投掷物类型": current_group["type"],
        "tick时间戳": current_group["tick"],
        "回合数": current_group["round_num"]
    }
    merged_data.append(merged_row)

    print(f"\n📊 合并后总数据：{len(merged_data)} 条")
    return merged_data


# ===================== 2. 初始化OpenAI客户端 =====================
def init_openai_client():
    if not OPENAI_API_KEY:
        raise ValueError("请先在虚拟环境中配置OPENAI_API_KEY环境变量")
    return OpenAI(
        api_key=OPENAI_API_KEY,
        base_url=OPENAI_BASE_URL
    )


# ===================== 3. 生成专属解说（烟雾弹+燃烧弹战术强化） =====================
def generate_commentary_openai(client, item):
    thrower = item["投掷人"]
    land_area = item["落点所在范围"]
    grenade_type = item["投掷物类型"]
    tick = item["tick时间戳"]
    side = item["投掷人所在队伍/阵营"]

    type_map = {
        "smoke": "烟雾弹",
        "inferno": "燃烧弹",
        "CFlashbangProjectile": "闪光弹",
        "CHEGrenadeProjectile": "手雷弹"
    }
    type_cn = type_map.get(grenade_type, "未知类型")

    # 【关键修改】优化提示词，要求AI按固定格式输出（带分隔符）
    system_prompt = """
    你是CS2职业比赛解说，仅描述烟雾弹和燃烧弹的战术运用，严格遵守：
    1. 内容包含：(1).投掷人不为“多个”时：投掷人+落点+类型+核心战术（烟雾弹侧重视野封锁/推进掩护，燃烧弹侧重区域控制/逼退对手/限制对手移动，闪光弹侧重致盲对手来制造击杀机会、逼退对手，高爆手雷侧重伤害敌人、压制对手）；
               (2).投掷人为“多个”时：采用被动句式，如：落点（此时也有多个）被覆盖/投掷/点上烟雾弹（替换为具体类型，类型只有一种），再分别阐述战术作用
       还可以加入的战术目的：抢占优势枪位、阻碍敌人推进、辅助/阻止下包等，根据详略适度选择
    2. 每次投掷生成三个详简版本，格式严格遵循（用---分隔，无额外内容）：
       短版（10-20字，可以省略战术作用，尽可能简洁）
       ---
       中版（25-40字，简述战术作用）
       ---
       长版（50-70字，详述战术作用）
    3. 语言多样化：替换动词（扔出/部署/打出/投掷）和战术描述，避免重复；
    4. 仅输出上述格式的解说文本，中性风格、口语化，无任何额外内容（如标题、标点、注释）。
    """

    user_prompt = f"""
    投掷事件信息：
    - 投掷人：{thrower}
    - 阵营/队伍：{side}
    - 落点地图范围：{land_area}
    - 投掷物类型：{type_cn}
    - tick时间戳：{tick}

    生成符合规则的解说文本：
    """

    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": system_prompt.strip()},
                {"role": "user", "content": user_prompt.strip()}
            ],
            temperature=0.9,
            max_tokens=300,  # 【关键】增大token上限，避免长版被截断
            frequency_penalty=0.2
        )
        commentary = response.choices[0].message.content.strip()

        # 【核心逻辑】拆分短/中/长版
        # 按---拆分，去除空行和多余空格
        versions = [v.strip() for v in commentary.split("---") if v.strip()]

        # 兜底：若AI输出格式异常，补充默认值
        short = versions[0] if len(versions) >= 1 else f"{thrower}（{side}）向{land_area}投掷{type_cn}！"
        medium = versions[1] if len(versions) >= 2 else f"{thrower}（{side}）向{land_area}扔出{type_cn}，压制对手活动！"
        long = versions[2] if len(
            versions) >= 3 else f"{thrower}（{side}）在{land_area}部署{type_cn}，有效封锁对手视野，阻碍推进！"

        #返回元组
        return short, medium, long


    except Exception as e:
        print(f"⚠️ tick{tick}生成失败：{str(e)[:30]}")
        # 异常时返回统一兜底的三个版本
        default_short = f"{thrower}（{side}）投掷{type_cn}至{land_area}！"
        default_medium = f"{thrower}（{side}）向{land_area}投掷{type_cn}，有效压制对手！"
        default_long = f"{thrower}（{side}）在{land_area}投掷{type_cn}，限制对手移动范围，抢占优势枪位！"
        return default_short, default_medium, default_long

# ===================== 4. 批量生成+保存结果 =====================
def batch_generate_commentary():
    # 初始化客户端
    client = init_openai_client()
    # 读取并合并烟雾弹+燃烧弹数据
    grenade_data = load_grenades_data()

    # 批量生成解说
    commentary_list = []
    total = len(grenade_data)

    priority_map={
        "smoke":5,
        "inferno":4,
        "CFlashbangProjectile":6,
        "CHEGrenadeProjectile":3
    }

    for idx, item in enumerate(grenade_data):
        type_cn = item["投掷物类型"]
        print(f"\r正在生成 {idx + 1}/{total} 条（{type_cn}）...", end="")

        # 生成单条解说
        short,medium,long = generate_commentary_openai(client, item)
        commentary_list.append({
            "event_id":f"{item["回合数"]}_grenade_{item["entity_id"]}",
            "round_num":item["回合数"],
            "start_time":item["tick时间戳"]/128,
            "end_time":1,
            "event_type":"grenade",
            "priority":priority_map[type_cn],
            "short_text_neutral":short,
            "medium_text_neutral":medium,
            "long_text_neutral":long,
        })

        # 限速（避免API超限）
        time.sleep(0.1)

    commentary_df = pd.DataFrame(commentary_list)
    #commentary_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")  # utf-8-sig避免中文乱码

    # 结果预览
    print(f"\n\n✅ 全部生成完成！共{len(commentary_list)}条（仅烟雾弹+燃烧弹）")
    print(f"📁 CSV文件保存至：{OUTPUT_CSV}")
    print("\n=== 前5条解说预览 ===")
    for i, res in enumerate(commentary_list[:5]):
        print(f"{i + 1}. 短版：{res['short_text_neutral']}")
        print(f"   中版：{res['medium_text_neutral']}")
        print(f"   长版：{res['long_text_neutral']}\n")

    return commentary_df
# ===================== 主流程 =====================
if __name__ == "__main__":
    try:
        batch_generate_commentary()
    except Exception as e:
        print(f"\n❌ 执行错误：{str(e)}")
        import traceback

        traceback.print_exc()