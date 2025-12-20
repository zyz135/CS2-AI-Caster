"""
所需要的接口函数为get_events_df。
该函数返回一个DataFrame，包含以下列：
    - event_id: 事件ID（格式：{round}_{event_type}_{index}）
    - round_num: 回合编号
    - start_time: 回合开始时间（秒）
    - end_time: 回合结束时间（秒）
    - event_type: 事件类型（2=经济分析；1=回合结束总结）
    - priority: 优先级
    - short_text_neutral: 短分析结果（10-30字）
    - medium_text_neutral: 中等分析结果（50字左右）
    - long_text_neutral: 长分析结果（70字左右）
731 lines中的<your demo name>需要替换为同目录下实际的demo文件名。
"""
from awpy import Demo
import polars as pl
import pandas as pd
import os
import re
from openai import OpenAI
from dotenv import load_dotenv


# ========== 物品名称中英文对照表 ==========
ITEM_NAME_CN = {
    # === 手枪 ===
    "glock": "格洛克",
    "hkp2000": "P2000",
    "usp_silencer": "USP消音",
    "p250": "P250",
    "elite": "双持贝瑞塔",
    "fiveseven": "FN57",
    "tec9": "Tec-9",
    "cz75a": "CZ75",
    "deagle": "沙漠之鹰",
    "revolver": "R8左轮",
    
    # === 冲锋枪 ===
    "mac10": "MAC-10",
    "mp9": "MP9",
    "mp7": "MP7",
    "mp5sd": "MP5消音",
    "ump45": "UMP-45",
    "p90": "P90",
    "bizon": "PP野牛",
    
    # === 步枪 ===
    "famas": "法玛斯",
    "galilar": "加利尔",
    "m4a1": "M4A1",
    "m4a1_silencer": "M4A1消音",
    "ak47": "AK-47",
    "sg556": "SG553",
    "aug": "AUG",
    "ssg08": "SSG08",
    "awp": "AWP",
    "scar20": "SCAR-20",
    "g3sg1": "G3SG1",
    
    # === 霰弹枪 ===
    "nova": "新星",
    "xm1014": "XM1014",
    "mag7": "MAG-7",
    "sawedoff": "截短霰弹枪",
    
    # === 机枪 ===
    "m249": "M249",
    "negev": "内格夫",
    
    # === 装备 ===
    "vest": "防弹衣",
    "vesthelm": "防弹衣+头盔",
    "defuser": "拆弹器",
    "taser": "电击枪",
    
    # === 投掷物 ===
    "hegrenade": "高爆手雷",
    "flashbang": "闪光弹",
    "smokegrenade": "烟雾弹",
    "molotov": "燃烧瓶",
    "incgrenade": "燃烧弹",
    "decoy": "诱饵弹",
    
    # === 其他 ===
    "c4": "C4炸弹",
    "knife": "刀",
    "knife_t": "刀",
}

# 回合结束原因中英文对照
REASON_CN = {
    "ct_killed": "CT全灭",
    "t_killed": "T全灭",
    "bomb_exploded": "炸弹爆炸",
    "bomb_defused": "炸弹拆除",
    "time_ran_out": "时间耗尽",
    "ct_surrender": "CT投降",
    "t_surrender": "T投降",
}


def get_item_cn(item_en: str) -> str:
    return ITEM_NAME_CN.get(item_en.lower(), item_en)


def get_reason_cn(reason: str) -> str:
    return REASON_CN.get(reason, reason)

#调用模型
def init_llm_client():
    """获取脚本所在目录以获得环境配置(api_key需要放在同目录下创建的api.env文件中)"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(script_dir, "api.env")
    load_dotenv(env_path)
    
    client = OpenAI(
        api_key=os.getenv("DASHSCOPE_API_KEY"),
        base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
    )
    return client


def parse_analysis_text(full_text: str) -> dict:
    """解析大模型输出，提取短、中、长三种分析结果"""
    result = {
        "short_text_neutral": "",
        "medium_text_neutral": "",
        "long_text_neutral": ""
    }
    
    # 尝试匹配不同格式的输出
    # 格式1: 短：xxx 中：xxx 长：xxx
    short_match = re.search(r'短[：:]\s*(.+?)(?=中[：:]|$)', full_text, re.DOTALL)
    medium_match = re.search(r'中[：:]\s*(.+?)(?=长[：:]|$)', full_text, re.DOTALL)
    long_match = re.search(r'长[：:]\s*(.+?)$', full_text, re.DOTALL)
    
    if short_match:
        result["short_text_neutral"] = short_match.group(1).strip()
    if medium_match:
        result["medium_text_neutral"] = medium_match.group(1).strip()
    if long_match:
        result["long_text_neutral"] = long_match.group(1).strip()
    
    # 如果没有匹配到，将整个文本作为长文本
    if not any([result["short_text_neutral"], result["medium_text_neutral"], result["long_text_neutral"]]):
        result["long_text_neutral"] = full_text.strip()
        result["medium_text_neutral"] = full_text.strip()[:50] if len(full_text) > 50 else full_text.strip()
        result["short_text_neutral"] = full_text.strip()[:30] if len(full_text) > 30 else full_text.strip()
    
    return result


def analyze_round_with_llm(client, round_data: str, map_name: str) -> dict:
    """使用大模型分析回合战术，返回解析后的分析结果"""
    
    system_prompt = f"""你是一位专业的CS2战术分析师，精通各种战术体系和经济管理策略。
你对于cs2的游戏规则了如指掌。
请根据提供的回合经济数据进行分析：（第一回合以及第十三回合无需给出分析，直接输出空字符串）
注意：1，经济数据的分析，只考虑本回合的起始资金以及上一回合的剩余资金和上一回合的购买记录）
2.格洛克以及USP消音为免费武器，购买记录以及起始装备中无需考虑这两者。
请分析：
1，分析本回合每个阵营的经济处于优势还是劣势；
2：导致该阵营经济劣势或者优势的原因；
3，目前经济情况下本回合可以采取的最好购买战术是什么（全起，eco（经济劣势情况下，为下一局的经济做准备），强起（关键比分回合或者打一个出其不意的效果），等）；
4，能否给队伍中的主力选手进行发枪；
5，上一回合是否保留了一定价值的枪械（如AWP，SCAR-20，G3SG1等）；
注意：以上方面不一定都要给出分析结果。
对于上述分析结果进行组织糅合，得出一段具有因果关系的结果，并且符合CS2的实际情况。
最终只需输出最后的综合分析结果。
综合分析结果给出短，中，长三种长度，格式如下：
短：[10-30字分析]
中：[50字左右分析]
长：[70字左右分析]

当前地图：{map_name}

"""

    completion = client.chat.completions.create(
        model="qwen3-max",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": round_data},
        ],
        stream=True
    )
    
    # 收集流式输出
    full_text = ""
    print("\n   战术分析：")
    for chunk in completion:
        content = chunk.choices[0].delta.content
        if content:
            full_text += content
            print(content, end="", flush=True)
    print("\n")
    
    # 解析并返回结果
    return parse_analysis_text(full_text)


def analyze_round_summary_with_llm(client, round_summary_data: str, map_name: str) -> dict:
    """使用大模型生成回合结束总结，返回解析后的分析结果"""
    
    system_prompt = f"""你是一位专业的CS2比赛解说员，擅长用简洁生动的语言总结比赛回合。
请根据提供的回合数据，生成回合结束总结。

总结要点：（无需考虑mvp的归属）
1. 回合胜负结果及原因
2. 关键击杀（首杀、多杀、残局）
3. 亮眼表现的选手
4. 战术执行情况（如有炸弹安装/拆除）

请进行总结，突出精彩时刻。
综合分析结果给出短，中，长三种长度，格式如下：
短：[10-30字，一句话总结]
中：[50字左右，包含关键信息]
长：[70字左右，详细描述]

当前地图：{map_name}

"""

    completion = client.chat.completions.create(
        model="qwen3-max",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": round_summary_data},
        ],
        stream=True
    )
    
    # 收集流式输出
    full_text = ""
    print("\n   回合总结：")
    for chunk in completion:
        content = chunk.choices[0].delta.content
        if content:
            full_text += content
            print(content, end="", flush=True)
    print("\n")
    
    # 解析并返回结果
    return parse_analysis_text(full_text)


def analyze_economy(demo_path: str, enable_llm: bool = True):
    """
    分析 demo 的经济数据
    
    Args:
        demo_path: demo 文件路径
        enable_llm: 是否启用大模型战术分析
    """
    print(f"正在加载 demo: {demo_path}")
    demo = Demo(demo_path)
    demo.parse()
    
    tickrate = demo.tickrate
    map_name = demo.header.get('map_name', 'unknown')
    print(f"Tickrate: {tickrate}")
    print(f"地图: {map_name}")
    print(f"回合数: {len(demo.rounds)}")
    print()
    
    # 初始化大模型客户端
    llm_client = None
    if enable_llm:
        try:
            llm_client = init_llm_client()
            print(" 大模型已连接，将进行战术分析")
        except Exception as e:
            print(f" 大模型连接失败: {e}，将跳过战术分析")
            enable_llm = False
    
    # ========== 1. 获取经济数据（包含阵营信息） ==========
    economy_fields = [
        "CCSPlayerController.CCSPlayerController_InGameMoneyServices.m_iAccount",           # 当前金钱
        "CCSPlayerController.CCSPlayerController_InGameMoneyServices.m_iStartAccount",      # 回合开始金钱
        "team_num",  # 阵营编号: 2=T, 3=CT
    ]
    
    print("正在解析经济数据...")
    economy_df = pl.from_pandas(demo.parser.parse_ticks(wanted_props=economy_fields))
    
    # 重命名列
    economy_df = economy_df.rename({
        "CCSPlayerController.CCSPlayerController_InGameMoneyServices.m_iAccount": "remaining_money",
        "CCSPlayerController.CCSPlayerController_InGameMoneyServices.m_iStartAccount": "start_money",
    })
    
    # 将 team_num 转换为阵营名称
    economy_df = economy_df.with_columns(
        pl.when(pl.col("team_num") == 2).then(pl.lit("T"))
        .when(pl.col("team_num") == 3).then(pl.lit("CT"))
        .otherwise(pl.lit("未知")).alias("side")
    )
    
    # ========== 2. 获取物品拾取/购买记录 ==========
    print("正在解析物品拾取记录...")
    item_pickup_df = pl.from_pandas(demo.parser.parse_event("item_pickup"))
    item_pickup_df = item_pickup_df.rename({
        "user_name": "name",
        "user_steamid": "steamid"
    })
    
    # ========== 2.1 获取击杀数据 ==========
    print("正在解析击杀数据...")
    kills_df = demo.kills
    
    # ========== 2.2 获取伤害数据 ==========
    print("正在解析伤害数据...")
    damages_df = demo.damages
    
    # ========== 2.3 获取炸弹事件 ==========
    print("正在解析炸弹事件...")
    bomb_df = demo.bomb
    
    # ========== 3. 为数据添加回合编号 ==========
    rounds_df = demo.rounds
    
    # 创建回合时间范围查找表
    round_ranges = []
    for row in rounds_df.to_dicts():
        round_ranges.append({
            "round_num": row["round_num"],
            "start": row["start"],
            "freeze_end": row["freeze_end"],
            "end": row["official_end"]
        })
    
    def get_round_num(tick, ranges):
        """根据 tick 获取回合编号"""
        for r in ranges:
            if r["start"] <= tick <= r["end"]:
                return r["round_num"]
        return None
    
    def is_freeze_time(tick, ranges):
        """判断是否在冻结时间内"""
        for r in ranges:
            if r["start"] <= tick <= r["freeze_end"]:
                return True
        return False
    
    # ========== 4. 获取每回合开始时的经济状态 ==========
    print("正在分析每回合经济状态...")
    
    round_economy_data = []
    
    for round_info in rounds_df.to_dicts():
        round_num = round_info["round_num"]
        freeze_end_tick = round_info["freeze_end"]
        
        # 获取冻结时间结束时刻的经济数据（代表回合开始时的装备状态）
        round_economy = economy_df.filter(
            (pl.col("tick") >= freeze_end_tick - 10) & 
            (pl.col("tick") <= freeze_end_tick + 10)
        )
        
        if len(round_economy) == 0:
            continue
            
        # 获取每个玩家在该时刻的数据
        player_economy = round_economy.group_by("name").agg([
            pl.col("start_money").first(),
            pl.col("remaining_money").first(),
            pl.col("steamid").first(),
            pl.col("side").first(),
        ])
        
        for player in player_economy.to_dicts():
            round_economy_data.append({
                "round_num": round_num,
                "name": player["name"],
                "steamid": player["steamid"],
                "start_money": player["start_money"],
                "remaining_money": player["remaining_money"],
                "side": player["side"],
            })
    
    round_economy_df = pl.DataFrame(round_economy_data)
    
    # ========== 5. 获取每回合的购买/拾取记录 ==========
    print("正在分析购买记录...")
    
    # 为 item_pickup 添加回合编号
    item_pickup_with_round = []
    for item in item_pickup_df.to_dicts():
        tick = item["tick"]
        rnum = get_round_num(tick, round_ranges)
        in_freeze = is_freeze_time(tick, round_ranges)
        if rnum is not None:
            item_pickup_with_round.append({
                **item,
                "round_num": rnum,
                "in_freeze_time": in_freeze,
                "is_purchase": in_freeze  # 冻结时间内的拾取视为购买
            })
    
    purchases_df = pl.DataFrame(item_pickup_with_round)
    
    # 过滤掉 knife 等非购买物品
    non_purchase_items = ["knife", "knife_t", "c4"]
    purchases_df = purchases_df.filter(~pl.col("item").is_in(non_purchase_items))
    
    # ========== 6. 输出分析结果 ==========
    print("\n" + "=" * 80)
    print("经济分析结果")
    print("=" * 80)
    
    # 获取所有玩家名单
    all_players = round_economy_df.select("name").unique().to_series().to_list()
    
    # 用于收集事件数据的列表
    events_data = []
    
    # 用于跟踪上一回合总结的 end_time
    prev_summary_end_time: float = 0.0
    
    for round_num in range(1, len(rounds_df) + 1):
        round_info = rounds_df.filter(pl.col("round_num") == round_num).to_dicts()[0]
        
        # 事件ID: {round}_{event_type}_{index}，event_type=2表示经济分析事件
        event_type = 2
        priority = 1
        
        # 计算经济分析的时间
        # 第一回合和第十三回合：start_time = end_time（无需分析）
        # 其他回合：start_time = 上一回合总结的 end_time, end_time = start_time + 3.5秒
        if round_num == 1:
            economy_start_time: float = 0.0
            economy_end_time: float = 0.0
        elif round_num == 13:
            # 第十三回合（攻防转换后第一回合）：start_time = end_time
            economy_start_time: float = prev_summary_end_time
            economy_end_time: float = economy_start_time  # start_time = end_time
        else:
            economy_start_time: float = prev_summary_end_time
            economy_end_time: float = economy_start_time + 3.5
        
        # 计算回合总结的时间
        # start_time = 本回合结束时的时间（秒）
        # end_time = start_time + 9秒
        round_end_seconds: float = round_info.get('official_end', 0) / tickrate if tickrate > 0 else 0.0
        summary_start_time: float = round_end_seconds
        summary_end_time: float = summary_start_time + 9.0
        
        # 更新上一回合总结的 end_time，供下一回合使用
        prev_summary_end_time = summary_end_time
        
        event_id = f"{round_num}_{event_type}_1"
        
        print(f"\n{'─' * 80}")
        winner = "CT" if round_info['winner'] == "ct" else "T"
        reason_cn = get_reason_cn(round_info['reason'])
        print(f"【第 {round_num} 回合】 获胜方: {winner}  原因: {reason_cn}")
        print(f"{'─' * 80}")
        
        # 获取该回合的经济数据
        round_eco = round_economy_df.filter(pl.col("round_num") == round_num)
        
        if len(round_eco) == 0:
            print("  [无数据]")
            continue
        
        # 按阵营分组显示
        ct_players = round_eco.filter(pl.col("side") == "CT").sort("name").to_dicts()
        t_players = round_eco.filter(pl.col("side") == "T").sort("name").to_dicts()
        
        for side_name, players in [("CT", ct_players), ("T", t_players)]:
            if not players:
                continue
            print(f"\n  【{side_name}方】")
            print("  ┌───────────────────────────────────────────────────────────────────────────────────┐")
            print("  │ 选手名           │ 起始金钱  │ 上回合剩余│ 起始装备                 │ 上回合购买              │")
            print("  ├───────────────────────────────────────────────────────────────────────────────────┤")
            
            for player in players:
                name = player["name"]
                start_money = player["start_money"]
                
                # 获取上一回合的剩余资金
                if round_num > 1:
                    prev_round_eco = round_economy_df.filter(
                        (pl.col("round_num") == round_num - 1) & 
                        (pl.col("name") == name)
                    )
                    if len(prev_round_eco) > 0:
                        prev_remaining_money = prev_round_eco.select("remaining_money").to_series().to_list()[0]
                    else:
                        prev_remaining_money = 0
                else:
                    prev_remaining_money = 0  # 第一回合没有上一回合数据
                
                # 获取该玩家上回合的购买记录（起始装备）
                if round_num > 1:
                    prev_round_purchases = purchases_df.filter(
                        (pl.col("round_num") == round_num - 1) & 
                        (pl.col("name") == name) &
                        (pl.col("is_purchase") == True)
                    )
                    start_equip_items = prev_round_purchases.select("item").to_series().to_list()
                else:
                    start_equip_items = []
                
                # 转换起始装备为中文
                start_equip_cn = [get_item_cn(item) for item in start_equip_items]
                # 过滤掉投掷物，只保留武器和防具
                weapons_armor = ["格洛克", "P2000", "USP消音", "P250", "双持贝瑞塔", "FN57", "Tec-9", 
                               "CZ75", "沙漠之鹰", "R8左轮", "MAC-10", "MP9", "MP7", "MP5消音", 
                               "UMP-45", "P90", "PP野牛", "法玛斯", "加利尔", "M4A1", "M4A1消音",
                               "AK-47", "SG553", "AUG", "SSG08", "AWP", "SCAR-20", "G3SG1",
                               "新星", "XM1014", "MAG-7", "截短霰弹枪", "M249", "内格夫",
                               "防弹衣", "防弹衣+头盔", "拆弹器"]
                start_equip_filtered = [item for item in start_equip_cn if item in weapons_armor]
                start_equip_str = ", ".join(start_equip_filtered[:3])
                if len(start_equip_filtered) > 3:
                    start_equip_str += f"..."
                
                # 获取该玩家上回合的购买记录
                if round_num > 1:
                    player_purchases = purchases_df.filter(
                        (pl.col("round_num") == round_num - 1) & 
                        (pl.col("name") == name) &
                        (pl.col("is_purchase") == True)
                    )
                    purchase_items = player_purchases.select("item").to_series().to_list()
                else:
                    purchase_items = []
                
                # 转换为中文名称
                purchase_items_cn = [get_item_cn(item) for item in purchase_items]
                purchase_str = ", ".join(purchase_items_cn[:3])
                if len(purchase_items_cn) > 3:
                    purchase_str += f"..."
                
                print(f"  │ {name:<16} │ ${start_money:>7} │ ${prev_remaining_money:>7} │ {start_equip_str:<24} │ {purchase_str:<23} │")
            
            print("  └───────────────────────────────────────────────────────────────────────────────────┘")
        
        # 使用大模型进行战术分析
        if enable_llm and llm_client:
            # 构建回合数据文本（按阵营分组）
            round_data_text = f"""
第 {round_num} 回合数据：
- 获胜方: {winner}
- 结束原因: {reason_cn}

"""
            for side_label, side_filter in [("CT方", "CT"), ("T方", "T")]:
                side_players = round_eco.filter(pl.col("side") == side_filter).sort("name").to_dicts()
                if not side_players:
                    continue
                    
                round_data_text += f"{side_label}选手经济状态：\n"
                for player in side_players:
                    name = player["name"]
                    start_money = player["start_money"]
                    
                    # 获取上一回合剩余资金
                    if round_num > 1:
                        prev_eco = round_economy_df.filter(
                            (pl.col("round_num") == round_num - 1) & 
                            (pl.col("name") == name)
                        )
                        prev_remaining = prev_eco.select("remaining_money").to_series().to_list()[0] if len(prev_eco) > 0 else 0
                    else:
                        prev_remaining = 0
                    
                    # 获取上回合购买记录
                    if round_num > 1:
                        prev_purchases = purchases_df.filter(
                            (pl.col("round_num") == round_num - 1) & 
                            (pl.col("name") == name) &
                            (pl.col("is_purchase") == True)
                        )
                        prev_items = [get_item_cn(i) for i in prev_purchases.select("item").to_series().to_list()]
                    else:
                        prev_items = []
                    
                    round_data_text += f"  - {name}: 起始${start_money}, 上回合剩余${prev_remaining}, "
                    round_data_text += f"上回合购买[{', '.join(prev_items) if prev_items else '无'}]\n"
                round_data_text += "\n"
            
            try:
                analysis_result = analyze_round_with_llm(llm_client, round_data_text, map_name)
                
                # 收集经济分析事件数据
                events_data.append({
                    "event_id": event_id,
                    "round_num": round_num,
                    "start_time": economy_start_time,
                    "end_time": economy_end_time,
                    "event_type": event_type,
                    "priority": priority,
                    "short_text_neutral": analysis_result["short_text_neutral"],
                    "medium_text_neutral": analysis_result["medium_text_neutral"],
                    "long_text_neutral": analysis_result["long_text_neutral"]
                })
            except Exception as e:
                print(f"\n   战术分析失败: {e}")
                # 即使失败也添加空记录
                events_data.append({
                    "event_id": event_id,
                    "round_num": round_num,
                    "start_time": economy_start_time,
                    "end_time": economy_end_time,
                    "event_type": event_type,
                    "priority": priority,
                    "short_text_neutral": "",
                    "medium_text_neutral": "",
                    "long_text_neutral": ""
                })
            
            # ========== 回合结束总结（event_type=1） ==========
            summary_event_type = 1
            summary_event_id = f"{round_num}_{summary_event_type}_1"
            summary_priority = 1  # 回合总结优先级较高
            
            # 获取本回合的击杀数据
            round_kills = kills_df.filter(pl.col("round_num") == round_num)
            ct_kills = round_kills.filter(pl.col("attacker_side") == "ct")
            t_kills = round_kills.filter(pl.col("attacker_side") == "t")
            
            # 获取本回合的炸弹事件
            round_start_tick = round_info.get('start', 0)
            round_end_tick = round_info.get('official_end', 0)
            round_bomb = bomb_df.filter(
                (pl.col("tick") >= round_start_tick) & 
                (pl.col("tick") <= round_end_tick)
            ) if len(bomb_df) > 0 else pl.DataFrame()
            
            # 构建回合总结数据文本
            summary_text = f"""
第 {round_num} 回合结束：
- 获胜方: {winner}
- 结束原因: {reason_cn}

击杀统计：
- CT方击杀: {len(ct_kills)} 人
- T方击杀: {len(t_kills)} 人

"""
            # 添加击杀详情
            if len(round_kills) > 0:
                summary_text += "击杀详情：\n"
                for kill in round_kills.sort("tick").to_dicts():
                    attacker = kill.get('attacker_name', '未知')
                    victim = kill.get('victim_name', '未知')
                    weapon = get_item_cn(kill.get('weapon', ''))
                    headshot = "（爆头）" if kill.get('headshot', False) else ""
                    summary_text += f"  - {attacker} 使用 {weapon} 击杀 {victim}{headshot}\n"
            
            # 添加炸弹事件
            if len(round_bomb) > 0:
                summary_text += "\n炸弹事件：\n"
                for bomb_event in round_bomb.to_dicts():
                    event_name = bomb_event.get('event', '')
                    player = bomb_event.get('player_name', '未知')
                    site = bomb_event.get('site', '')
                    if 'plant' in event_name.lower():
                        summary_text += f"  - {player} 在 {site} 点安装炸弹\n"
                    elif 'defuse' in event_name.lower():
                        summary_text += f"  - {player} 拆除炸弹\n"
                    elif 'explode' in event_name.lower():
                        summary_text += f"  - 炸弹爆炸\n"
            
            # 计算MVP（击杀最多的玩家）
            if len(round_kills) > 0:
                mvp_stats = round_kills.group_by("attacker_name").agg(pl.len().alias("kills"))
                if len(mvp_stats) > 0:
                    mvp = mvp_stats.sort("kills", descending=True).to_dicts()[0]
                    summary_text += f"\n本回合MVP: {mvp['attacker_name']} ({mvp['kills']} 杀)\n"
            
            try:
                summary_result = analyze_round_summary_with_llm(llm_client, summary_text, map_name)
                
                # 收集回合总结事件数据
                events_data.append({
                    "event_id": summary_event_id,
                    "round_num": round_num,
                    "start_time": summary_start_time,  # 回合总结在回合结束时
                    "end_time": summary_end_time,      # start_time + 9秒
                    "event_type": summary_event_type,
                    "priority": summary_priority,
                    "short_text_neutral": summary_result["short_text_neutral"],
                    "medium_text_neutral": summary_result["medium_text_neutral"],
                    "long_text_neutral": summary_result["long_text_neutral"]
                })
            except Exception as e:
                print(f"\n   回合总结失败: {e}")
                events_data.append({
                    "event_id": summary_event_id,
                    "round_num": round_num,
                    "start_time": summary_start_time,
                    "end_time": summary_end_time,
                    "event_type": summary_event_type,
                    "priority": summary_priority,
                    "short_text_neutral": "",
                    "medium_text_neutral": "",
                    "long_text_neutral": ""
                })
    
    # ========== 7. 创建事件 DataFrame ==========
    events_df = pd.DataFrame(events_data)
    
    return events_df


def get_events_df(demo_path: str, enable_llm: bool = True) -> pd.DataFrame:
    """
    获取经济分析事件 DataFrame 的接口
    
    Args:
        demo_path: demo 文件路径
        enable_llm: 是否启用大模型战术分析，默认为 True
    
    Returns:
        pd.DataFrame: 事件分析数据
    """
    return analyze_economy(demo_path, enable_llm=enable_llm)


if __name__ == "__main__":    
    # 获取脚本所在目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 分析 demo（与脚本在同一目录下）
    demo_path = os.path.join(script_dir, "<your demo name>")
    
    # 使用接口获取 events_df
    events_df = get_events_df(demo_path, enable_llm=True)
    

