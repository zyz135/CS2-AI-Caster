import pandas as pd
import numpy as np
import os
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

def process_dem_file(dem_path, verbose=False):
    """
    处理DEM文件并返回标准格式的DataFrame（接口函数）
    
    参数:
        dem_path (str): DEM文件路径
        verbose (bool): 是否输出详细处理信息，默认为False
        
    返回:
        pandas.DataFrame: 包含以下9列的DataFrame:
            - event_id: 事件唯一ID，格式"round_[回合号]_kill_[序号]"
            - round_num: 回合号（整数）
            - start_time: 事件开始时间（秒，float）
            - end_time: 事件结束时间（秒，float）
            - event_type: 事件类型（固定为"kill"）
            - priority: 优先级（int，6-7，数值越大越重要）
            - short_text_neutral: 短中性解说文本（15字内）
            - medium_text_neutral: 中中性解说文本（30字内）
            - long_text_neutral: 长中性解说文本（50字内）
    
    示例:
        >>> df = process_dem_file("match.dem", verbose=True)
        >>> print(df.shape)  # (行数, 9)
    """
    if verbose:
        print("=" * 60)
        print(f"📂 正在处理DEM文件: {os.path.basename(dem_path)}")
        print("=" * 60)
        print("🔧 正在解析DEM文件...")
    
    # ========== 第1步：解析DEM文件 ==========
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
    
    # ========== 第2步：初始化工具 ==========
    mapper = PositionMapper()
    
    # 武器名称映射表
    weapon_map = {
        'glock': '格洛克', 'ak47': 'AK-47', 'm4a1': 'M4A1', 
        'm4a1_silencer': '消音M4', 'awp': 'AWP', 'usp_silencer': 'USP消音版',
        'deagle': '沙漠之鹰', 'elite': '双枪', 'famas': '法玛斯',
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
    
    # ========== 第3步：创建标准格式的DataFrame ==========
    standard_events = []
    
    for idx, row in raw_df.iterrows():
        # 1. 获取基本信息
        attacker = row.get('attacker_name', 'Unknown')
        victim = row.get('victim_name', 'Unknown')
        weapon = row.get('weapon', 'Unknown')
        
        # 2. 获取回合号
        round_num = 1
        round_cols = ['round_num', 'round', 'round_number']
        for col in round_cols:
            if col in row and not pd.isna(row[col]):
                try:
                    round_num = int(row[col])
                    break
                except:
                    continue
        
        # 3. 获取tick和时间
        tick = row.get('tick', idx * 128)
        start_time = tick / 128.0
        end_time = start_time + 0.5
        
        # 4. 映射武器名称
        weapon_cn = weapon_map.get(weapon.lower(), weapon)
        
        # 5. 映射位置
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
        
        # 6. 检查是否为爆头
        is_headshot = False
        for headshot_col in ['headshot', 'is_headshot', 'isHeadshot']:
            if headshot_col in row and not pd.isna(row[headshot_col]):
                is_headshot = bool(row[headshot_col])
                break
        
        # 7. 检查是否有助攻
        has_assist = False
        assister_name = None
        assist_cols = ['assister_name', 'assisterName', 'assister']
        for col in assist_cols:
            if col in row and not pd.isna(row[col]) and row[col] not in ['', 'None', 'null']:
                has_assist = True
                assister_name = row[col]
                break
        
        # 8. 生成事件ID
        event_id = f"round_{round_num}_kill_{idx+1:03d}"
        
        # 9. 确定优先级（6-7级）
        priority = 6  # 默认优先级6
        if is_headshot:
            priority = 7
        elif weapon.lower() in ['awp', 'ssg08', 'scar20', 'g3sg1']:  # 狙击枪
            priority = 7
        
        # 10. 生成解说文本
        short_text = f"{attacker}击杀了{victim}"
        
        medium_text = f"{attacker}在{attacker_place}使用{weapon_cn}击杀了{victim}"
        
        if has_assist and assister_name:
            long_text = f"{assister_name}提供助攻，{attacker}在{attacker_place}使用{weapon_cn}{'爆头' if is_headshot else ''}击杀了在{victim_place}的{victim}"
        else:
            long_text = f"{attacker}在{attacker_place}使用{weapon_cn}{'爆头' if is_headshot else ''}击杀了在{victim_place}的{victim}"
        
        # 11. 限制文本长度
        short_text = short_text[:15]
        medium_text = medium_text[:30]
        long_text = long_text[:50]
        
        # 12. 创建标准格式的事件记录（仅9个字段）
        standard_event = {
            'event_id': event_id,
            'round_num': round_num,
            'start_time': round(start_time, 2),
            'end_time': round(end_time, 2),
            'event_type': 'kill',
            'priority': priority,
            'short_text_neutral': short_text,
            'medium_text_neutral': medium_text,
            'long_text_neutral': long_text
        }
        
        standard_events.append(standard_event)
    
    # ========== 第4步：创建并排序DataFrame ==========
    df = pd.DataFrame(standard_events)
    
    if not df.empty:
        # 按回合号和时间排序
        df = df.sort_values(['round_num', 'start_time']).reset_index(drop=True)
        
        # 重新生成事件ID（排序后）
        for i, row in df.iterrows():
            df.at[i, 'event_id'] = f"round_{row['round_num']}_kill_{i+1:03d}"
    
    if verbose:
        if df.empty:
            print("❌ 生成的DataFrame为空")
        else:
            print(f"✅ 成功创建标准格式DataFrame!")
            print(f"📈 形状: {df.shape[0]} 行 × {df.shape[1]} 列")
            print(f"🔢 回合数: {df['round_num'].nunique()}")
            print(f"⭐ 优先级分布: {dict(df['priority'].value_counts())}")
            print("\n📋 DataFrame列结构:")
            for i, col in enumerate(df.columns, 1):
                print(f"  {i:2d}. {col:20s}")
    
    return df


