import pandas as pd
import os
import json
import glob
import concurrent.futures
from openai import OpenAI

# ================= 配置区域 =================
MY_API_KEY = "sk-c2435a4ac2574b4e8ef61ef0c3da7ed4"  # 你的 Key
MODEL_NAME = "qwen3-max" 
BLACKLIST_KEYWORDS = [
    "摔死", "自杀", "未知", "world", "World", "Trigger", "entity", "Bot", "BOT"
]
# ===========================================

def get_machine_style_prompt_with_timing(events_batch):
    """
    构建带有时长约束的 Prompt
    events_batch 结构: [{"id": 0, "text": "原文...", "duration": 4.0}, ...]
    """
    events_str = json.dumps(events_batch, ensure_ascii=False)
    
    return f"""
你现在是CS2知名主播“玩机器”（Machine）。
请将以下【解说原文】重写为【玩机器直播风格】，并**严格遵守时长限制**。

【人设要求】
1. **风格**：阴阳怪气、玩梗（FlameZ=火仔, ZywOo=载物/薯薯，zweih=哲伟，枪械用简称，拒绝机械感。
2. **核心约束**：每条数据都有 "duration" (秒)。**主播语速约为 5字/秒**。
   - 如果 duration=3.0s，字数不能超过 15 字。
   - 如果 duration=8.0s，可以展开讲，但不要废话。
   - **超时是解说的大忌！必须精简！**
3. **过滤**：遇到“摔死/自杀”直接返回空字符串 ""。
4. 闪光弹就叫闪或闪光（不要叫闪bang），烟雾弹叫烟，燃烧弹和燃烧瓶叫火，AK-47叫AK，M4A4叫A4，M4A1-S叫A1，
AWP叫大狙，沙漠之鹰叫沙鹰，ssg-08叫鸟狙，以及一些常用的说法和昵称上网搜寻
5. 说明选手所在点位用点位名就行了，不要出现“缓冲区”等机械感字眼
【输入数据】(JSON):
{events_str}

【输出格式】(纯JSON列表，只包含重写后的文本字符串，顺序对应):
["重写后的短句(3s)", "重写后的长句(8s)", ""]
"""

def calculate_duration(time_range_str):
    """从 '15.0-19.0s' 中计算时长"""
    try:
        t_str = str(time_range_str).replace("s", "").strip()
        start, end = t_str.split('-')
        return float(end) - float(start)
    except:
        return 4.0 # 默认兜底

def process_batch(client, batch_df):
    """处理批次"""
    # 1. 构建包含时长的输入结构
    input_events = []
    original_texts = batch_df['解说文本'].tolist()
    
    for idx, row in batch_df.iterrows():
        text = str(row['解说文本'])
        duration = calculate_duration(row['时间范围'])
        
        # 预过滤
        if any(bad in text for bad in BLACKLIST_KEYWORDS):
            text = "SKIP_THIS_EVENT"
            
        input_events.append({
            "text": text,
            "duration": round(duration, 1) # 保留1位小数
        })

    # 如果全是被过滤的
    if all(e["text"] == "SKIP_THIS_EVENT" for e in input_events):
        return [""] * len(input_events)

    # 2. 调用 LLM
    try:
        prompt = get_machine_style_prompt_with_timing(input_events)
        
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.8,
            response_format={"type": "json_object"} if "qwen" not in MODEL_NAME else None
        )
        
        content = response.choices[0].message.content
        content = content.replace("```json", "").replace("```", "").strip()
        
        try:
            new_texts = json.loads(content)
        except:
            # 简单的列表补救
            import ast
            if "[" in content:
                start = content.find("[")
                end = content.rfind("]") + 1
                new_texts = ast.literal_eval(content[start:end])
            else:
                return original_texts

        # 长度对齐
        if len(new_texts) != len(original_texts):
            # 简单的截断或补齐
            if len(new_texts) > len(original_texts):
                new_texts = new_texts[:len(original_texts)]
            else:
                new_texts.extend([""] * (len(original_texts) - len(new_texts)))
                
        return new_texts

    except Exception as e:
        print(f"❌ LLM 调用失败: {e}")
        return original_texts

def process_file(filepath):
    print(f"\n⏱️  正在进行[限时]风格化重写: {os.path.basename(filepath)} ...")
    try:
        df = pd.read_csv(filepath, encoding='utf-8-sig')
        if df.empty: return

        # 同样排除已处理文件
        if "_machine_style" in filepath: return

        client = OpenAI(api_key=MY_API_KEY, base_url="https://dashscope.aliyuncs.com/compatible-mode/v1")
        
        batch_size = 8 # 批次稍微小一点，让模型更专注
        batches = [df[i:i + batch_size] for i in range(0, df.shape[0], batch_size)]
        
        styled_texts = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_batch = {executor.submit(process_batch, client, batch): i for i, batch in enumerate(batches)}
            
            results_map = {}
            for future in concurrent.futures.as_completed(future_to_batch):
                batch_idx = future_to_batch[future]
                try:
                    res = future.result()
                    results_map[batch_idx] = res
                    print(f"   ✅ 批次 {batch_idx + 1}/{len(batches)} 完成")
                except Exception as e:
                    print(f"   ❌ 批次 {batch_idx} 失败: {e}")
                    results_map[batch_idx] = batches[batch_idx]['解说文本'].tolist()

        for i in range(len(batches)):
            styled_texts.extend(results_map.get(i, []))
            
        df['原解说'] = df['解说文本']
        df['解说文本'] = styled_texts
        
        # 过滤空行
        df_final = df[df['解说文本'].astype(str).str.strip() != ""]
        
        new_path = filepath.replace(".csv", "_machine_style.csv")
        df_final.to_csv(new_path, index=False, encoding='utf-8-sig')
        print(f"🎉 完美！输出: {new_path}")
        
    except Exception as e:
        print(f"❌ 失败: {e}")

def main():
    print("🔍 扫描 data 目录...")
    search_pattern = os.path.join("data", "**", "final_*_half.csv")
    files = glob.glob(search_pattern, recursive=True)
    target_files = [f for f in files if "_machine_style" not in f]
    
    for f in target_files:
        process_file(f)

if __name__ == "__main__":
    main()