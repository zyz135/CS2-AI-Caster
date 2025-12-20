import pandas as pd
import os
import random
import uuid
import json
from openai import OpenAI

# ================= 配置区域 =================
LLM_API_KEY = "" 
LLM_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
FREEZE_TIME_THRESHOLD = 15  # 跳过开局冻结时间

# 🚨 正式模式开关：False = 调用大模型生成文本; True = 只生成提示词用于省钱调试
DEBUG_ONLY_PROMPTS = False 

SYSTEM_PROMPT = """
你是一名专业的 CS2 战术分析解说。请根据提供的比赛实时数据生成解说文本。

【输入数据说明】
你将看到 T/CT 具体的站位（如“二楼上”、“长箱”）以及【战术态势分析】。
【战术态势分析】是后台基于宏观区域（如A缓冲区、B区）计算得出的内部情报，**仅供你理解局势，严禁在解说文本中直接念出“A缓冲区”或“macro”等技术词汇。**

【解说逻辑要求】
1. **未下包时**：重点关注 T 方动向。
   - 如果 T 多人在“缓冲区”，解说应描述为“T方正在集结”、“意图对A/B动手”。
   - 如果 T 多人在“区/包点”，解说应描述为“大军压境”、“已经攻入”。
   - 关注“中路”的 T，描述他们控制地图的意图。
2. **已下包时**：攻守互换，重点关注 CT 方回防站位，忽略 T 的进攻意图。

【输出格式】
必须返回 JSON 对象，包含 short, medium, long 三个版本。
"""

# ================= 辅助函数 =================

def analyze_macro_intent(df_state, c4_planted):
    """基于宏观区域分析战术意图"""
    if c4_planted:
        ct_df = df_state[df_state['side'] == 'CT']
        a_ct = len(ct_df[ct_df['location_macro'].str.contains('A', na=False)])
        b_ct = len(ct_df[ct_df['location_macro'].str.contains('B', na=False)])
        mid_ct = len(ct_df[ct_df['location_macro'].str.contains('中路', na=False)])
        
        focus = []
        if a_ct > 0: focus.append(f"{a_ct}名CT尝试回防A区")
        if b_ct > 0: focus.append(f"{b_ct}名CT尝试回防B区")
        if mid_ct > 0: focus.append(f"{mid_ct}名CT在中路寻找机会")
        
        return f"CT回防态势: {', '.join(focus)}" if focus else "CT正在整备回防"
    else:
        t_df = df_state[df_state['side'] == 'T']
        t_a_buffer = len(t_df[t_df['location_macro'] == 'A缓冲区'])
        t_a_site = len(t_df[t_df['location_macro'] == 'A区'])
        t_b_buffer = len(t_df[t_df['location_macro'] == 'B缓冲区'])
        t_b_site = len(t_df[t_df['location_macro'] == 'B区'])
        t_mid = len(t_df[t_df['location_macro'] == '中路'])
        
        intent_desc = []
        if t_a_site > 0: intent_desc.append(f"T方{t_a_site}人已杀入A区(极强意图)")
        elif t_a_buffer >= 2: intent_desc.append(f"T方{t_a_buffer}人在A外围集结(进攻A区意图明显)")
            
        if t_b_site > 0: intent_desc.append(f"T方{t_b_site}人已杀入B区(极强意图)")
        elif t_b_buffer >= 2: intent_desc.append(f"T方{t_b_buffer}人在B外围集结(进攻B区意图明显)")
            
        if t_mid >= 1: intent_desc.append(f"T方{t_mid}人控中路")
            
        if not intent_desc: return "T方正在匪家或后点默认架枪，暂无明显动向"
        return " | ".join(intent_desc)

def summarize_window(df_window):
    """提取时间窗口内的关键信息"""
    last_sec = df_window['second'].max()
    df_state = df_window[df_window['second'] == last_sec]
    
    t_locs = df_state[df_state['side'] == 'T']['location_name'].value_counts().to_dict()
    ct_locs = df_state[df_state['side'] == 'CT']['location_name'].value_counts().to_dict()
    t_alive = len(df_state[(df_state['side'] == 'T') & (df_state['health'] > 0)])
    ct_alive = len(df_state[(df_state['side'] == 'CT') & (df_state['health'] > 0)])
    
    c4_planted = False
    if 'is_c4_planted' in df_window.columns:
        col = df_window['is_c4_planted']
        if col.dtype == 'bool': c4_planted = col.any()
        else: c4_planted = col.astype(str).str.contains("True|true", case=False).any()
    
    game_phase = "C4已安放 (CT回防)" if c4_planted else "C4未安放 (T进攻)"
    tactical_insight = analyze_macro_intent(df_state, c4_planted)

    utils_list = []
    active_utils = df_window['active_utility'].dropna().unique()
    for u_str in active_utils:
        if not u_str: continue
        items = [item.strip() for item in u_str.split('|')]
        utils_list.extend(items)
    
    return {
        "time_range": f"{df_window['second'].min()}-{df_window['second'].max()}s",
        "t_alive": t_alive,
        "ct_alive": ct_alive,
        "t_locs": t_locs,
        "ct_locs": ct_locs,
        "game_phase": game_phase,
        "tactical_insight": tactical_insight,
        "utils": list(set(utils_list))
    }

def call_llm_for_three_versions(summary):
    """返回: (json_result, user_prompt_string)"""
    
    user_prompt = f"""
时间: {summary['time_range']}
当前阶段: {summary['game_phase']}
【战术态势分析(内部情报)】: {summary['tactical_insight']}
T方({summary['t_alive']}人)具体站位: {summary['t_locs']}
CT方({summary['ct_alive']}人)具体站位: {summary['ct_locs']}
生效道具: {summary['utils']}

请根据【战术态势分析】判断主攻方向，结合【具体站位】生成 short, medium, long 三个版本的解说文本。
"""

    # === 调试模式拦截 ===
    if DEBUG_ONLY_PROMPTS:
        return {"short": "[DEBUG] Skipped", "medium": "[DEBUG] Skipped", "long": "[DEBUG] Skipped"}, user_prompt

    # === 正式调用 ===
    if not LLM_API_KEY: return None, "No API Key"

    try:
        client = OpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)
        response = client.chat.completions.create(
            model="qwen-max", 
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT}, 
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.7,
            response_format={"type": "json_object"} 
        )
        return json.loads(response.choices[0].message.content), user_prompt
        
    except Exception as e:
        print(f"❌ LLM Error: {e}")
        return None, f"Error: {e}"

# ================= 核心分析逻辑 =================

def run_tactical_analysis(input_csv, output_dir, target_rounds=None):
    mode_str = f"测试模式 (Rounds: {target_rounds})" if target_rounds else "全场模式"
    if DEBUG_ONLY_PROMPTS:
        print(f"🚧 [DEBUG模式] 仅生成提示词")
    else:
        print(f"🎥 [正式模式] 正在调用大模型生成文本...")

    if not os.path.exists(input_csv):
        print(f"❌ 找不到输入文件: {input_csv}")
        return

    df = pd.read_csv(input_csv)
    # 填充空值
    if 'active_utility' in df.columns: df['active_utility'] = df['active_utility'].fillna("")
    if 'location_macro' not in df.columns:
        print("❌ 错误：缺少 'location_macro' 列，请先运行 Force 模式重新提取数据！")
        return
    
    all_rounds = sorted(df['round_num'].unique())
    if target_rounds:
        rounds_to_process = [r for r in target_rounds if r in all_rounds]
    else:
        rounds_to_process = all_rounds

    results = []
    prompt_logs = [] 
    
    running_time = 0.0
    is_second_half = False

    for r_num in all_rounds:
        if r_num == 13 and not is_second_half:
            running_time = 0.0
            is_second_half = True
            if target_rounds is None: print("🔄 下半场开始，时间轴重置。")

        round_df = df[df['round_num'] == r_num]
        max_sec = round_df['second'].max()
        should_process = (r_num in rounds_to_process)

        if should_process:
            print(f"\n   ⚙️ 正在分析 Round {r_num} ...")
            current_start = 0.0
            
            while current_start < max_sec:
                # 跳过冻结时间
                if current_start < FREEZE_TIME_THRESHOLD:
                    current_start = FREEZE_TIME_THRESHOLD
                    continue
                
                step = random.randint(6, 10)
                current_end = min(current_start + step, max_sec)
                
                win_df = round_df[(round_df['second'] >= current_start) & (round_df['second'] < current_end)]
                
                if not win_df.empty:
                    summary = summarize_window(win_df)
                    
                    # 🚀 正式调用 LLM
                    texts, used_prompt = call_llm_for_three_versions(summary)
                    
                    if texts:
                        global_start_time = running_time + current_start
                        row = {
                            "event_id": str(uuid.uuid4()),
                            "round_num": r_num,
                            "start_time": round(global_start_time, 2),
                            "priority": 2,
                            "short_text_neutral": texts.get("short", ""),
                            "medium_text_neutral": texts.get("medium", ""),
                            "long_text_neutral": texts.get("long", "")
                        }
                        results.append(row)
                        
                        # 控制台只打印 Medium 预览，防止刷屏
                        print(f"      ⏱️ [{row['start_time']}s] [{summary['game_phase']}]")
                        print(f"      🎙️ {row['medium_text_neutral']}")
                        print("-" * 40)
                        
                        # 依然保存 prompt 日志，方便事后复盘
                        prompt_logs.append(f"=== Round {r_num} | Time {summary['time_range']} ===\n{used_prompt}\n\n")

                current_start = current_end
                if max_sec - current_start <= 0.5: break
                
        running_time += max_sec

    # 保存最终 CSV
    if not results: return
    final_df = pd.DataFrame(results)
    cols = ["event_id", "round_num", "start_time", "priority", "short_text_neutral", "medium_text_neutral", "long_text_neutral"]
    final_df = final_df[cols]

    if target_rounds:
        save_path = os.path.join(output_dir, "test_tactical_result.csv")
        final_df.to_csv(save_path, index=False, encoding="utf-8-sig")
        print(f"\n✅ 测试结果已保存: {save_path}")
    else:
        df_h1 = final_df[final_df['round_num'] <= 12]
        df_h2 = final_df[final_df['round_num'] >= 13]
        path_h1 = os.path.join(output_dir, "tactical_part1.csv")
        path_h2 = os.path.join(output_dir, "tactical_part2.csv")
        if not df_h1.empty: df_h1.to_csv(path_h1, index=False, encoding="utf-8-sig")
        if not df_h2.empty: df_h2.to_csv(path_h2, index=False, encoding="utf-8-sig")
        print(f"\n✅ 全部分析完成！解说文件已生成。")

    # 保存 prompt 日志
    log_path = os.path.join(output_dir, "debug_prompts.txt")
    with open(log_path, "w", encoding="utf-8") as f:
        f.writelines(prompt_logs)