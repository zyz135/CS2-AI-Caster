import pandas as pd
import numpy as np
import os
import requests
import json
from datetime import datetime
from awpy import Demo
import warnings
warnings.filterwarnings('ignore')

class PositionMapper:
    """位置映射器"""
    def __init__(self):
        self.anchors = pd.DataFrame([
            {"name": "匪家", "x": 1177.03, "y": -805.52, "z": -186.91},
            {"name": "匪家", "x": 1252.87, "y": -75.45, "z": -103.97},
            {"name": "A1（近匪家）", "x": 828.81, "y": -1081.92, "z": -199.03},
            {"name": "A1", "x": 446.97, "y": -1589.34, "z": -177.58},
            {"name": "三明治", "x": -317.62, "y": -1528.13, "z": -103.97},
            {"name": "二楼上", "x": 151.97, "y": -1914.05, "z": 24.03},
            {"name": "二楼下", "x": 151.96, "y": -1914.06, "z": -103.97},
            {"name": "匪二楼", "x": 258.95, "y": -2326.49, "z": 24.03},
            {"name": "匪二楼里", "x": 956.60, "y": -1831.24, "z": -7.97},
            {"name": "死点", "x": -284.19, "y": -2411.58, "z": -99.97},
            {"name": "短箱", "x": -386.77, "y": -2106.80, "z": -115.97},
            {"name": "长箱", "x": -699.14, "y": -2134.63, "z": -115.97},
            {"name": "长椅", "x": -815.80, "y": -1786.43, "z": -87.93},
            {"name": "匪跳", "x": -142.97, "y": -1418.03, "z": -8.18},
            {"name": "跳台", "x": -477.97, "y": -1556.13, "z": 24.03},
            {"name": "跳台下", "x": -574.64, "y": -1552.39, "z": -103.97},
            {"name": "拱门", "x": -664.70, "y": -1062.40, "z": -142.90},
            {"name": "Jungle", "x": -1018.81, "y": -1407.23, "z": -102.88},
            {"name": "VIP", "x": -1193.63, "y": -932.99, "z": -103.97},
            {"name": "VIP", "x": -1209.91, "y": -606.79, "z": -103.97},
            {"name": "忍者位", "x": -381.52, "y": -2394.97, "z": -101.86},
            {"name": "A包", "x": -483.10, "y": -2217.06, "z": -115.58},
            {"name": "警亭", "x": -874.69, "y": -2541.74, "z": 28.03},
            {"name": "警家", "x": -1598.12, "y": -1093.28, "z": -168.30},
            {"name": "警家", "x": -1638.86, "y": -1897.44, "z": -204.00},
            {"name": "超市", "x": -1972.04, "y": -589.91, "z": -103.97},
            {"name": "外围", "x": -2073.78, "y": -84.52, "z": -101.97},
            {"name": "外围", "x": -2019.19, "y": 546.17, "z": -101.87},
            {"name": "沙发", "x": -2503.79, "y": 301.76, "z": -103.97},
            {"name": "包点箱子上", "x": -1937.28, "y": 383.02, "z": -45.67},
            {"name": "沙发贴墙", "x": -2581.64, "y": 535.92, "z": -103.68},
            {"name": "白车", "x": -2325.00, "y": 791.48, "z": -62.05},
            {"name": "B2", "x": -1642.45, "y": 762.03, "z": 16.03},
            {"name": "厨房", "x": -1095.93, "y": 434.22, "z": -15.97},
            {"name": "B准备区", "x": -207.14, "y": 810.73, "z": -72.49},
            {"name": "下水道", "x": -995.72, "y": -25.57, "z": -303.97},
            {"name": "VIP下", "x": -1039.20, "y": -677.45, "z": -199.97},
            {"name": "长凳", "x": -842.61, "y": -788.93, "z": -159.63},
            {"name": "拱门外", "x": -518.33, "y": -788.89, "z": -192.55},
            {"name": "L位", "x": -363.97, "y": -935.25, "z": -102.39},
            {"name": "中路", "x": 18.88, "y": -684.43, "z": -126.82},
            {"name": "沙袋", "x": 370.59, "y": -667.39, "z": -100.82},
            {"name": "匪口", "x": 348.99, "y": 104.12, "z": -166.20},
            {"name": "A1上", "x": 815.93, "y": -1704.38, "z": -44.97},
            {"name": "小黑屋", "x": -1199.90, "y": -163.18, "z": 8.03},
            {"name": "B小", "x": -776.07, "y": -386.46, "z": -103.99},
            {"name": "B小", "x": -435.30, "y": -411.66, "z": -103.26},
            {"name": "B小", "x": -1046.10, "y": 189.09, "z": -108.87},
        ])
    
    def map_position(self, x, y, z):
        """将坐标映射到最近的游戏点位"""
        if pd.isna(x) or pd.isna(y) or pd.isna(z):
            return None
        
        closest = [float('inf'), None, None]
        for row in self.anchors.itertuples(index=False):
            dist_square = (row.x - x) ** 2 + (row.y - y) ** 2
            if dist_square < closest[0]:
                closest = [dist_square, row.name, row.z]
            elif dist_square == closest[0]:
                if abs(row.z - z) <= abs(closest[2] - z):
                    closest = [dist_square, row.name, row.z]
        
        return closest[1]

class QwenAPIClient:
    """Qwen API客户端"""
    def __init__(self, api_key=None, model="qwen3-max"):
        self.api_key = api_key
        self.model = model
        self.base_url = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
    
    def call_api(self, prompt, system_prompt=None):
        """调用Qwen API"""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        
        messages.append({"role": "user", "content": prompt})
        
        data = {
            "model": self.model,
            "input": {"messages": messages},
            "parameters": {
                "result_format": "message",
                "max_tokens": 2000,
                "temperature": 0.5,
                "top_p": 0.9
            }
        }
        
        try:
            response = requests.post(self.base_url, headers=headers, json=data, timeout=30)
            if response.status_code == 200:
                result = response.json()
                if 'output' in result and 'choices' in result['output']:
                    return result['output']['choices'][0]['message']['content']
            return None
        except:
            return None

def analyze_kill_contexts(events_df):
    """分析击杀上下文，包括多杀、补枪等"""
    context_info = {}
    
    if events_df.empty:
        return context_info
    
    # 按时间排序
    events_sorted = events_df.sort_values('tick').reset_index(drop=True)
    
    # 存储每个玩家的击杀链
    player_kill_chains = {}
    
    for i in range(len(events_sorted)):
        current_event = events_sorted.iloc[i]
        current_attacker = current_event['attacker']
        current_victim = current_event['victim']
        current_tick = current_event['tick']
        
        # ========== 1. 多杀检测 ==========
        if current_attacker not in player_kill_chains:
            player_kill_chains[current_attacker] = []
        
        # 清理过时的击杀（超过2秒）
        player_kill_chains[current_attacker] = [
            kill for kill in player_kill_chains[current_attacker]
            if (current_tick - kill['tick']) <= 256  # 2秒内
        ]
        
        # 添加当前击杀到击杀链
        current_kill_info = {
            'tick': current_tick,
            'victim': current_victim,
            'weapon': current_event['weapon'],
            'is_headshot': current_event['is_headshot'],
            'kill_type': current_event.get('kill_type', 'normal')  # 添加击杀类型
        }
        
        player_kill_chains[current_attacker].append(current_kill_info)
        
        # 计算当前连杀数
        current_chain = player_kill_chains[current_attacker]
        kill_count = len(current_chain)
        
        # ========== 2. 补枪检测 ==========
        is_trade_kill = False
        trade_details = None
        
        # 检查当前受害者是否在1秒内刚完成过击杀
        if current_victim in player_kill_chains:
            victim_kills = player_kill_chains[current_victim]
            if victim_kills:
                latest_victim_kill = victim_kills[-1]
                time_since_victim_kill = current_tick - latest_victim_kill['tick']
                
                # 如果受害者1秒内刚完成击杀，这就是补枪
                if time_since_victim_kill <= 128:  # 1秒内
                    is_trade_kill = True
                    trade_details = {
                        'time_since_victim_kill': time_since_victim_kill / 128.0,
                        'is_quick_trade': time_since_victim_kill <= 64  # 0.5秒内
                    }
        
        # ========== 3. 构建上下文 ==========
        context = {
            # 多杀信息
            'is_multikill': kill_count > 1,
            'kill_chain_length': kill_count,
            'kill_chain': current_chain.copy(),
            'is_chain_start': kill_count == 1,
            'time_since_last_kill': 0 if kill_count == 1 else (current_tick - current_chain[-2]['tick']) / 128.0,
            
            # 补枪信息（只在有补枪时设置）
            'is_trade_kill': is_trade_kill,
            'trade_details': trade_details if is_trade_kill else None,
            
            # 击杀类型
            'kill_type': current_event.get('kill_type', 'normal')
        }
        
        context_info[i] = context
    
    return context_info

def get_kill_description(kill_type, attacker, victim, weapon):
    """根据击杀类型获取合适的描述"""
    if kill_type == 'c4':
        return f"{victim}被C4炸死"
    elif kill_type == 'suicide':
        return f"{attacker}自尽"
    elif kill_type == 'team_kill':
        return f"{attacker}击杀了队友{victim}"
    elif kill_type == 'world':
        return f"{victim}阵亡"
    elif kill_type == 'grenade':
        return f"{attacker}用手雷击杀了{victim}"
    elif kill_type == 'molotov':
        return f"{attacker}用燃烧瓶击杀了{victim}"
    else:
        return f"{attacker}用{weapon}击杀了{victim}"

def generate_commentary_texts(qwen_client, event_info, kill_context, verbose=False):
    """
    使用Qwen API生成简洁的解说文本
    """
    attacker = event_info['attacker']
    victim = event_info['victim']
    weapon = event_info['weapon']
    attacker_place = event_info['attacker_place']
    victim_place = event_info['victim_place']
    is_headshot = event_info['is_headshot']
    has_assist = event_info['has_assist']
    assister_name = event_info['assister_name'] if has_assist else None
    kill_type = event_info.get('kill_type', 'normal')
    
    # 获取击杀上下文
    kill_chain_length = kill_context['kill_chain_length']
    is_trade_kill = kill_context['is_trade_kill']
    
    # 如果是特殊击杀类型（world/c4/自杀等），直接生成描述
    if kill_type in ['c4', 'suicide', 'team_kill', 'world']:
        return generate_special_kill_texts(event_info, kill_context)
    
    # 构建系统提示 - 简洁风格
    system_prompt = """你是专业的CS2比赛解说员，请生成简洁、中性的解说文本。

## 核心要求：
*******最重要的**********:weapon_map = {
        'glock': '格洛克', 'ak47': 'AK', 'm4a1': 'M4', 
        'm4a1_silencer': 'M4', 'awp': '大狙', 'usp_silencer': 'USP',
        'deagle': '沙鹰', 'elite': '双枪', 'famas': '法玛斯',
        'galilar': '咖喱', 'mac10': 'MAC-10', 'mp9': 'MP9',
        'ump45': '车王', 'p90': 'P90', 'mp7': 'MP7',
        'p250': 'P250', 'tec9': 'TEC-nine', 'fiveseven': '57',
        'hegrenade': '雷', 'inferno': '燃烧弹', 'flashbang': '闪光',
        'smokegrenade': '烟', 'molotov': '燃烧瓶', 'incgrenade': '燃烧瓶',
        'knife': '刀', 'taser': '电击枪', 'nova': '新星',
        'xm1014': '连喷', 'mag7': 'MAG-7', 'sawedoff': '截短霰弹枪',
        'bizon': '野牛', 'negev': '内格夫', 'm249': 'M249',
        'hkp2000': 'P2000', 'usp': 'USP', 'cz75a': 'CZ75',
        'revolver': 'R8', 'mp5sd': 'MP5-SD', 'aug': 'AUG',
        'sg556': 'SG 553', 'scar20': '连狙', 'g3sg1': '连狙',
        'ssg08': 'SSG 08'
    }武器名称替换用这个规则，左侧是数据中的武器名称，右侧是解说中使用的名称。***************
1. **简洁直接**：只说必要信息，不说"本场第一杀"、"非爆头"等多余描述
2. **补枪识别**：只在确实有补枪时才说"补枪"，不说"无补枪"、"暂无补枪风险"
3. **爆头说明**：只有爆头时才说"爆头"，普通击杀不说
4. **多杀渐进**：
   - 第1杀：正常描述
   - 第2杀：说"双杀"
   - 第3杀：说"三杀"
   - 第4杀：说"四杀"
   - 第5杀：说"五杀ACE"
5. **不说时间**：不说"仅用时X秒"、"快速"等时间描述
6. **助攻融入**：将助攻自然地融入句子中
7. **字数限制**：严格遵循

## 输出格式：
{
  "short_text": "短文本（15字内）",
  "medium_text": "中文本（30字内）",
  "long_text": "长文本（50字内）"
}"""
    
    # 构建用户提示 - 只给必要信息
    prompt = f"""生成CS2击杀解说文本：

## 基本信息
- 击杀者：{attacker}
- 被击杀者：{victim}
- 武器：{weapon}
- 击杀者位置：{attacker_place}
- 被击杀者位置：{victim_place}
- 爆头：{"是" if is_headshot else "否"}
- 助攻：{assister_name if has_assist else "无"}

## 上下文
- 当前连杀数：{kill_chain_length}
- 是否补枪：{"是" if is_trade_kill else "否"}

## 要求：
1. 简洁直接，只说击杀事实
2. 只有爆头才说"爆头"
3. 只有补枪才说"补枪"
4. 多杀时才说"双杀/三杀"等
5. 不说时间、不说多余描述
6. 助攻信息自然地融入句子中

## 示例：
正常击杀：s1mple在A包点使用大狙击杀了device
爆头击杀：ZywOo使用ak在长箱爆头击杀了flameZ
补枪击杀：device使用m4z在B包点补枪击杀了s1mple
带助攻：device在chopper助攻下击杀了s1mple

## 输出JSON格式："""
    
    if verbose:
        trade_info = "（补枪）" if is_trade_kill else ""
        head_info = "（爆头）" if is_headshot else ""
        print(f"  🤖 正在生成{attacker}的第{kill_chain_length}杀{head_info}{trade_info}...")
    
    # 调用API
    response = qwen_client.call_api(prompt, system_prompt)
    
    if response:
        try:
            # 尝试解析JSON
            import re
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                json_str = json_match.group()
                result = json.loads(json_str)
                
                if all(key in result for key in ['short_text', 'medium_text', 'long_text']):
                    short_text = result['short_text'][:15]
                    medium_text = result['medium_text'][:30]
                    long_text = result['long_text'][:50]
                    
                    # 验证：补枪击杀必须包含"补枪"
                    if is_trade_kill and "补枪" not in short_text:
                        return generate_local_texts_simple(event_info, kill_context)
                    
                    # 验证：爆头击杀必须包含"爆头"
                    if is_headshot and "爆头" not in short_text:
                        return generate_local_texts_simple(event_info, kill_context)
                    
                    return short_text, medium_text, long_text
        except:
            pass
    
    # 如果API调用失败，使用本地生成
    return generate_local_texts_simple(event_info, kill_context)

def generate_special_kill_texts(event_info, kill_context):
    """生成特殊击杀类型的文本（C4、自杀等）"""
    attacker = event_info['attacker']
    victim = event_info['victim']
    kill_type = event_info.get('kill_type', 'normal')
    kill_chain_length = kill_context['kill_chain_length']
    
    # 根据击杀类型生成描述
    if kill_type == 'c4':
        if attacker == victim:  # 自己炸死自己
            short_text = f"{victim}被C4炸死"[:15]
            medium_text = short_text
            long_text = short_text
        else:
            short_text = f"{attacker}用C4炸死{victim}"[:15]
            medium_text = short_text
            long_text = short_text
    elif kill_type == 'suicide':
        short_text = f"{attacker}自尽"[:15]
        medium_text = short_text
        long_text = short_text
    elif kill_type == 'team_kill':
        short_text = f"{attacker}击杀队友{victim}"[:15]
        medium_text = short_text
        long_text = short_text
    elif kill_type == 'world':
        short_text = f"{victim}阵亡"[:15]
        medium_text = short_text
        long_text = short_text
    elif kill_type == 'grenade':
        short_text = f"{attacker}手雷击杀{victim}"[:15]
        medium_text = f"{attacker}投掷手雷击杀了{victim}"[:30]
        long_text = medium_text
    elif kill_type == 'molotov':
        short_text = f"{attacker}燃烧瓶击杀{victim}"[:15]
        medium_text = f"{attacker}投掷燃烧瓶击杀了{victim}"[:30]
        long_text = medium_text
    else:
        # 默认情况
        short_text = f"{victim}阵亡"[:15]
        medium_text = short_text
        long_text = short_text
    
    return short_text, medium_text, long_text

def generate_local_texts_simple(event_info, kill_context):
    """本地生成简洁文本（备用）"""
    attacker = event_info['attacker']
    victim = event_info['victim']
    weapon = event_info['weapon']
    attacker_place = event_info['attacker_place']
    victim_place = event_info['victim_place']
    is_headshot = event_info['is_headshot']
    has_assist = event_info['has_assist']
    assister_name = event_info['assister_name'] if has_assist else None
    kill_type = event_info.get('kill_type', 'normal')
    
    kill_chain_length = kill_context['kill_chain_length']
    is_trade_kill = kill_context['is_trade_kill']
    
    # 如果是特殊击杀类型
    if kill_type in ['c4', 'suicide', 'team_kill', 'world']:
        return generate_special_kill_texts(event_info, kill_context)
    
    # 基础描述
    headshot_text = "爆头" if is_headshot else ""
    assist_text = f"{assister_name}助攻下" if has_assist and assister_name else ""
    
    # 多杀前缀
    if kill_chain_length == 1:
        multikill_text = ""
    elif kill_chain_length == 2:
        multikill_text = "双杀"
    elif kill_chain_length == 3:
        multikill_text = "三杀"
    elif kill_chain_length == 4:
        multikill_text = "四杀"
    elif kill_chain_length == 5:
        multikill_text = "五杀ACE"
    elif kill_chain_length >= 6:
        multikill_text = f"{kill_chain_length}杀"
    else:
        multikill_text = ""
    
    # 补枪文本
    trade_text = "补枪" if is_trade_kill else ""
    
    # 组合前缀
    prefix_parts = []
    if multikill_text:
        prefix_parts.append(multikill_text)
    if trade_text:
        prefix_parts.append(trade_text)
    
    prefix = "".join(prefix_parts)
    if prefix:
        prefix = f"{prefix}，"
    
    # 生成短文本
    if is_headshot:
        if has_assist:
            short_text = f"{attacker}在{assist_text}{headshot_text}击杀{victim}"[:15]
        else:
            short_text = f"{attacker}{headshot_text}击杀{victim}"[:15]
    elif is_trade_kill:
        short_text = f"{attacker}{trade_text}击杀{victim}"[:15]
    elif has_assist:
        short_text = f"{attacker}在{assist_text}击杀{victim}"[:15]
    else:
        short_text = f"{attacker}击杀{victim}"[:15]
    
    # 生成中文本
    if attacker_place != "未知位置" and victim_place != "未知位置":
        if has_assist:
            medium_text = f"{prefix}{attacker}在{assister_name}助攻下于{attacker_place}{headshot_text}击杀在{victim_place}的{victim}"[:30]
        else:
            medium_text = f"{prefix}{attacker}在{attacker_place}{headshot_text}击杀在{victim_place}的{victim}"[:30]
    else:
        if has_assist:
            medium_text = f"{prefix}{attacker}在{assister_name}助攻下{headshot_text}击杀{victim}"[:30]
        else:
            medium_text = f"{prefix}{attacker}{headshot_text}击杀{victim}"[:30]
    
    # 生成长文本
    if attacker_place != "未知位置" and victim_place != "未知位置":
        if has_assist:
            long_text = f"{prefix}{attacker}在{assister_name}助攻下于{attacker_place}使用{weapon}{headshot_text}击杀在{victim_place}的{victim}"[:50]
        else:
            long_text = f"{prefix}{attacker}在{attacker_place}使用{weapon}{headshot_text}击杀在{victim_place}的{victim}"[:50]
    else:
        if has_assist:
            long_text = f"{prefix}{attacker}在{assister_name}助攻下使用{weapon}{headshot_text}击杀{victim}"[:50]
        else:
            long_text = f"{prefix}{attacker}使用{weapon}{headshot_text}击杀{victim}"[:50]
    
    return short_text, medium_text, long_text

def process_dem_file(dem_path, api_key, model="qwen3-max", verbose=False):
    """
    处理DEM文件并返回标准格式的DataFrame
    """
    if verbose:
        print("=" * 80)
        print(f"📂 正在处理DEM文件: {os.path.basename(dem_path)}")
        print("=" * 80)
        print("🔧 正在解析DEM文件...")
    
    # 解析DEM文件
    try:
        dem = Demo(dem_path, tickrate=128)
        dem.parse()
    except Exception as e:
        if verbose:
            print(f"❌ DEM文件解析失败: {e}")
        return pd.DataFrame()
    
    # 转换为pandas DataFrame
    raw_df = dem.kills.to_pandas()
    
    if verbose:
        print(f"✅ 解析完成，找到 {len(raw_df)} 条击杀记录")
        print("🗺️  正在处理位置信息...")
    
    # 初始化工具
    mapper = PositionMapper()
    qwen_client = QwenAPIClient(api_key, model)
    
    # 武器名称映射表
    weapon_map = {
        'glock': '格洛克', 'ak47': 'AK-47', 'm4a1': 'M4A1', 
        'm4a1_silencer': '消音M4', 'awp': 'AWP', 'usp_silencer': 'USP消音版',
        'deagle': '沙鹰', 'elite': '双枪', 'famas': '法玛斯',
        'galilar': '加利尔', 'mac10': 'MAC-10', 'mp9': 'MP9',
        'ump45': 'UMP-45', 'p90': 'P90', 'mp7': 'MP7',
        'p250': 'P250', 'tec9': 'TEC-9', 'fiveseven': 'FN57',
        'hegrenade': '手雷', 'inferno': '燃烧弹', 'flashbang': '闪光弹',
        'smokegrenade': '烟雾弹', 'molotov': '燃烧瓶', 'incgrenade': '燃烧瓶',
        'knife': '刀', 'taser': '电击枪', 'nova': '新星',
        'xm1014': 'XM1014', 'mag7': 'MAG-7', 'sawedoff': '截短霰弹枪',
        'bizon': '野牛', 'negev': '内格夫', 'm249': 'M249',
        'hkp2000': 'P2000', 'usp': 'USP', 'cz75a': 'CZ75',
        'revolver': 'R8左轮', 'mp5sd': 'MP5-SD', 'aug': 'AUG',
        'sg556': 'SG 553', 'scar20': 'SCAR-20', 'g3sg1': 'G3SG1',
        'ssg08': 'SSG 08'
    }
    
    # 特殊武器/击杀类型处理
    special_weapons = {
        'planted_c4': 'C4爆炸',
        'world': '环境伤害',
        'worldspawn': '环境伤害'
    }
    
    # 提取基本信息
    basic_events = []
    
    for idx, row in raw_df.iterrows():
        # 基本信息
        attacker = row.get('attacker_name', 'Unknown')
        victim = row.get('victim_name', 'Unknown')
        weapon = row.get('weapon', 'Unknown')
        
        # 回合号
        round_num = 1
        round_cols = ['round_num', 'round', 'round_number']
        for col in round_cols:
            if col in row and not pd.isna(row[col]):
                try:
                    round_num = int(row[col])
                    break
                except:
                    continue
        
        # tick和时间
        tick = row.get('tick', idx * 128)
        start_time = tick / 128.0
        end_time = start_time + 0.5
        
        # 检测击杀类型
        kill_type = 'normal'
        weapon_lower = weapon.lower()
        
        if weapon_lower in ['planted_c4', 'c4']:
            kill_type = 'c4'
            weapon_cn = 'C4'
        elif weapon_lower in ['world', 'worldspawn']:
            kill_type = 'world'
            weapon_cn = '环境伤害'
        elif weapon_lower == 'hegrenade':
            kill_type = 'grenade'
            weapon_cn = '手雷'
        elif weapon_lower in ['molotov', 'incgrenade', 'inferno']:
            kill_type = 'molotov'
            weapon_cn = '燃烧瓶'
        elif attacker == victim:  # 自杀
            kill_type = 'suicide'
            weapon_cn = weapon_map.get(weapon_lower, weapon)
        elif weapon_lower in weapon_map:
            weapon_cn = weapon_map[weapon_lower]
        else:
            weapon_cn = weapon
        
        # 检查是否为队友击杀
        # 注意：这里需要实际队伍信息，暂时简化处理
        if 'attacker_team' in row and 'victim_team' in row:
            if row['attacker_team'] == row['victim_team']:
                kill_type = 'team_kill'
        
        # 位置映射
        attacker_place = None
        victim_place = None
        
        if all(col in row for col in ['attacker_X', 'attacker_Y', 'attacker_Z']):
            attacker_place = mapper.map_position(
                row['attacker_X'], row['attacker_Y'], row['attacker_Z']
            )
        
        if all(col in row for col in ['victim_X', 'victim_Y', 'victim_Z']):
            victim_place = mapper.map_position(
                row['victim_X'], row['victim_Y'], row['victim_Z']
            )
        
        attacker_place = attacker_place or "未知位置"
        victim_place = victim_place or "未知位置"
        
        # 检查爆头
        is_headshot = False
        for headshot_col in ['headshot', 'is_headshot', 'isHeadshot']:
            if headshot_col in row and not pd.isna(row[headshot_col]):
                is_headshot = bool(row[headshot_col])
                break
        
        # 检查助攻
        has_assist = False
        assister_name = None
        assist_cols = ['assister_name', 'assisterName', 'assister']
        for col in assist_cols:
            if col in row and not pd.isna(row[col]) and row[col] not in ['', 'None', 'null']:
                has_assist = True
                assister_name = row[col]
                break
        
        # 优先级
        priority = 6  # 默认优先级6
        if is_headshot:
            priority = 7
        elif weapon.lower() in ['awp', 'ssg08', 'scar20', 'g3sg1']:  # 狙击枪
            priority = 7
        elif kill_type in ['c4', 'suicide', 'team_kill']:  # 特殊击杀类型优先级较低
            priority = 5
        
        # 存储基本信息
        basic_event = {
            'idx': idx,
            'round_num': round_num,
            'tick': tick,
            'start_time': start_time,
            'end_time': end_time,
            'attacker': attacker,
            'victim': victim,
            'weapon': weapon_cn,
            'attacker_place': attacker_place,
            'victim_place': victim_place,
            'is_headshot': is_headshot,
            'has_assist': has_assist,
            'assister_name': assister_name,
            'kill_type': kill_type,  # 添加击杀类型
            'priority': priority
        }
        
        basic_events.append(basic_event)
    
    # 转换为临时DataFrame用于上下文分析
    temp_df = pd.DataFrame(basic_events)
    
    if verbose:
        print("🔍 正在分析击杀上下文...")
    
    # 分析击杀上下文
    kill_contexts = analyze_kill_contexts(temp_df)
    
    # 统计
    stats = {
        'multikills': sum(1 for ctx in kill_contexts.values() if ctx['is_multikill']),
        'trade_kills': sum(1 for ctx in kill_contexts.values() if ctx['is_trade_kill']),
        'headshots': sum(1 for event in basic_events if event['is_headshot']),
        'special_kills': sum(1 for event in basic_events if event['kill_type'] != 'normal')
    }
    
    if verbose:
        print(f"✅ 分析完成:")
        print(f"  多杀: {stats['multikills']} 个")
        print(f"  补枪: {stats['trade_kills']} 个")
        print(f"  爆头: {stats['headshots']} 个")
        print(f"  特殊击杀: {stats['special_kills']} 个")
        print("🤖 正在生成解说文本...")
    
    # 生成解说文本
    standard_events = []
    
    for idx, basic_event in enumerate(basic_events):
        # 获取击杀上下文
        context = kill_contexts.get(idx, {
            'is_multikill': False, 
            'kill_chain_length': 1,
            'is_trade_kill': False,
            'kill_type': 'normal'
        })
        
        # 生成事件ID
        event_id = f"round_{basic_event['round_num']}_kill_{idx+1:03d}"
        
        # 准备事件信息
        event_info = {
            'attacker': basic_event['attacker'],
            'victim': basic_event['victim'],
            'weapon': basic_event['weapon'],
            'attacker_place': basic_event['attacker_place'],
            'victim_place': basic_event['victim_place'],
            'is_headshot': basic_event['is_headshot'],
            'has_assist': basic_event['has_assist'],
            'assister_name': basic_event['assister_name'],
            'kill_type': basic_event['kill_type']
        }
        
        # 生成文本
        short_text, medium_text, long_text = generate_commentary_texts(
            qwen_client, event_info, context, verbose
        )
        
        # 创建标准格式事件
        standard_event = {
            'event_id': event_id,
            'round_num': basic_event['round_num'],
            'start_time': round(basic_event['start_time'], 2),
            'end_time': round(basic_event['end_time'], 2),
            'event_type': 'kill',
            'priority': basic_event['priority'],
            'short_text_neutral': short_text,
            'medium_text_neutral': medium_text,
            'long_text_neutral': long_text,
            'kill_type': basic_event['kill_type']  # 保留击杀类型用于调试
        }
        
        standard_events.append(standard_event)
        
        # 进度显示
        if verbose and (idx + 1) % 10 == 0:
            kill_type_info = f" ({basic_event['kill_type']})" if basic_event['kill_type'] != 'normal' else ""
            print(f"  📝 已生成 {idx + 1}/{len(basic_events)} 个事件{kill_type_info}")
    
    # 创建并排序DataFrame
    df = pd.DataFrame(standard_events)
    
    if not df.empty:
        # 按回合号和时间排序
        df = df.sort_values(['round_num', 'start_time']).reset_index(drop=True)
        
        # 重新生成事件ID
        for i, row in df.iterrows():
            df.at[i, 'event_id'] = f"round_{row['round_num']}_kill_{i+1:03d}"
    
    if verbose:
        if df.empty:
            print("❌ 生成的DataFrame为空")
        else:
            print(f"✅ 成功创建DataFrame!")
            print(f"📈 形状: {df.shape[0]} 行 × {df.shape[1]} 列")
            print(f"🔢 回合数: {df['round_num'].nunique()}")
            print(f"⭐ 优先级分布: 5级-{(df['priority'] == 5).sum()}个, 6级-{(df['priority'] == 6).sum()}个, 7级-{(df['priority'] == 7).sum()}个")
    
    return df

def set_api_key(key):
    global GLOBAL_API_KEY
    GLOBAL_API_KEY = key
    return GLOBAL_API_KEY