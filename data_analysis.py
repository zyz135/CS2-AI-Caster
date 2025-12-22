import pandas as pd
import os
import random
import uuid
import json
from openai import OpenAI

# ================= 配置区域 =================
LLM_API_KEY = None
def setAPI(API_KEY):
    global LLM_API_KEY
    LLM_API_KEY = API_KEY

LLM_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
FREEZE_TIME_THRESHOLD = 15 
DEBUG_ONLY_PROMPTS = False 

SYSTEM_PROMPT = """
你是一名专业的 CS2 战术分析解说。
【重要原则】
1. 你的任务是根据实时数据生成解说，但必须**严格遵守当前的“解说切入点”**。
2. 即使场面静止，也要通过切换视角（如从宏观转到微观，从现在的站位转到对未来的预测）来避免废话。
3. 输出 JSON: {"short": "...", "medium": "...", "long": "..."}
"""

# 定义随机切入点池
FOCUS_ANGLES = [
    "MACRO: 分析双方的地图控制权分布 (Map Control)。",
    "MICRO: 挑选一名关键位置的选手，点评他的站位细节 (Player Specific)。",
    "PREDICTION: 基于当前站位，预测接下来最可能爆发交火的点位 (Prediction)。",
    "UTILITY: 重点关注当前的道具覆盖情况，或者指出哪边缺道具 (Utility)。",
    "TENSION: 描述当前的静默博弈和心理压力，不要只报点 (Psychological)。",
    "SUMMARY: 简要总结当前的攻防阵型 (Formation)。"
]

def analyze_macro_intent(df_state, c4_planted):
    if c4_planted: return "C4已安放-回防阶段"
    t_players = df_state[df_state['side'] == 'T']
    if t_players.empty: return "T方全灭"
    locs = t_players['location_macro'].value_counts()
    if locs.empty: return "默认架枪"
    top_loc = locs.idxmax()
    if locs.max() >= 3: return f"T方重兵集结于{top_loc}"
    return "T方分散控图"

def summarize_window(df_window):
    if df_window.empty: return None
    last_sec = df_window['second'].max()
    df_state = df_window[df_window['second'] == last_sec]
    
    t_alive = len(df_state[(df_state['side'] == 'T') & (df_state['health'] > 0)])
    ct_alive = len(df_state[(df_state['side'] == 'CT') & (df_state['health'] > 0)])
    
    c4_planted = False
    if 'is_c4_planted' in df_window.columns:
        col = df_window['is_c4_planted']
        c4_planted = col.any() if col.dtype == 'bool' else col.astype(str).str.contains("True|true", case=False).any()
    
    intent = analyze_macro_intent(df_state, c4_planted)
    
    def get_loc_str(side):
        p = df_state[df_state['side'] == side]
        locs = p['location_name'].value_counts().to_dict()
        return ", ".join([f"{k}({v})" for k,v in locs.items()])
    
    utils = []
    if 'active_utility' in df_window.columns:
        raw_utils = df_window['active_utility'].dropna().unique()
        for u in raw_utils:
            if u and str(u).strip(): utils.append(str(u))
            
    return {
        "time_range": f"{df_window['second'].min()}-{df_window['second'].max()}s",
        "t_alive": t_alive, "ct_alive": ct_alive,
        "t_locs": get_loc_str('T'), "ct_locs": get_loc_str('CT'),
        "game_phase": intent, "utils": " | ".join(utils[:3])
    }

def call_llm_for_three_versions(summary, focus_angle):
    if not summary or not LLM_API_KEY: return None, "Error"
    
    user_prompt = f"""
    Time: {summary['time_range']}
    Phase: {summary['game_phase']}
    T Pos: {summary['t_locs']}
    CT Pos: {summary['ct_locs']}
    Utils: {summary['utils']}
    
    【解说指令】: 请务必从以下角度进行解说 -> {focus_angle}
    (不要在输出中包含"从XX角度"字样，直接输出解说内容)
    """
    
    try:
        client = OpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)
        response = client.chat.completions.create(
            model="qwen-max", 
            messages=[{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user_prompt}],
            temperature=0.8,
            response_format={"type": "json_object"} 
        )
        content = response.choices[0].message.content.replace("```json", "").replace("```", "").strip()
        return json.loads(content), user_prompt
    except Exception as e: return None, str(e)

# ================= 核心分析逻辑 (修改版) =================

def run_tactical_analysis(input_csv, output_dir, target_rounds=None):
    if not os.path.exists(input_csv): return pd.DataFrame()
    
    # 1. 定义缓存文件路径
    cache_file = os.path.join(output_dir, "tactical_gen_cache.csv")
    
    # === 🔥 关键修改：如果有缓存，直接返回，绝不生成！ ===
    if os.path.exists(cache_file):
        try:
            df_cache = pd.read_csv(cache_file, encoding="utf-8-sig")
            if not df_cache.empty:
                print(f"🎥 [Tactical] 🚀 检测到现有战术缓存 ({len(df_cache)}条)，直接加载！")
                print(f"            (如果想重新生成，请手动删除 data/xxx/tactical_gen_cache.csv)")
                return df_cache
        except Exception as e:
            print(f"⚠️ 缓存读取失败: {e}，将重新生成...")

    # ===================================================
    # 下面是正常的生成逻辑，只有当文件不存在时才会执行
    # ===================================================
    
    df = pd.read_csv(input_csv)
    all_rounds = sorted(df['round_num'].unique())
    rounds = [r for r in target_rounds if r in all_rounds] if target_rounds else all_rounds
    running_time = 0.0
    is_second_half = False

    print(f"🎥 [Tactical] 未找到缓存，开始生成战术分析...")

    last_angle_idx = -1

    for r_num in all_rounds:
        if r_num == 13 and not is_second_half: running_time = 0.0; is_second_half = True
        round_df = df[df['round_num'] == r_num]
        max_sec = round_df['second'].max()
        
        if r_num in rounds:
            print(f"\n   ⚙️ Round {r_num} ...")
            current_start = FREEZE_TIME_THRESHOLD
            
            while current_start < max_sec:
                step = random.randint(6, 10)
                current_end = min(current_start + step, max_sec)
                global_start = running_time + current_start
                
                win_df = round_df[(round_df['second'] >= current_start) & (round_df['second'] < current_end)]
                if not win_df.empty:
                    summary = summarize_window(win_df)
                    
                    while True:
                        idx = random.randint(0, len(FOCUS_ANGLES) - 1)
                        if idx != last_angle_idx:
                            last_angle_idx = idx
                            break
                    chosen_angle = FOCUS_ANGLES[idx]
                    
                    texts, _ = call_llm_for_three_versions(summary, chosen_angle)
                    
                    if texts:
                        row = {
                            "event_id": str(uuid.uuid4()), "round_num": r_num,
                            "start_time": round(global_start, 5), "priority": 5,
                            "short_text_neutral": texts.get("short", ""),
                            "medium_text_neutral": texts.get("medium", ""),
                            "long_text_neutral": texts.get("long", "")
                        }
                        # 实时写入缓存
                        pd.DataFrame([row]).to_csv(cache_file, mode='a', index=False, header=not os.path.exists(cache_file), encoding="utf-8-sig")
                        angle_tag = chosen_angle.split(":")[0]
                        print(f"      [{angle_tag}] 保存: {row['start_time']}s")

                current_start = current_end
                if max_sec - current_start <= 1.0: break
        running_time += max_sec

    print(f"\n✅ 分析完成！")
    return pd.read_csv(cache_file, encoding="utf-8-sig") if os.path.exists(cache_file) else pd.DataFrame()