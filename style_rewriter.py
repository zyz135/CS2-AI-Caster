import pandas as pd
import os
import json
import re
import concurrent.futures
from openai import OpenAI
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# ================= 配置区域 =================
MY_API_KEY = os.getenv("DASHSCOPE_API_KEY") 
# 建议使用 qwen-max 以获得更好的风格遵循能力，如果想省钱可以用 qwen-plus
MODEL_NAME = "qwen3-max" 
BATCH_SIZE = 10  # 批处理大小
TARGET_SPEED = 3.5 # 目标语速：每秒 X 个字 (玩机器语速稍快，设为3.5比较自然)

# 黑名单关键词（如果原文包含这些，可能需要特殊处理或过滤）
BLACKLIST_KEYWORDS = [
    "摔死", "自杀", "未知", "world", "World", "Trigger", "entity", "Bot", "BOT"
]
# ===========================================

def parse_duration(time_range_str):
    """从 '10.5-15.2s' 格式中解析持续时间"""
    try:
        clean = str(time_range_str).lower().replace("s", "").strip()
        start, end = clean.split("-")
        duration = float(end) - float(start)
        return max(1.5, duration) # 最少给1.5秒，防止除以0或过短
    except:
        return 4.0 # 默认兜底时长

def get_machine_style_prompt(events_batch):
    """
    构建包含【语速限制】的玩机器风格 Prompt
    """
    # 将 DataFrame 行转为字典列表，并注入字数限制
    context_data = []
    for item in events_batch:
        duration = item['duration']
        # 计算目标字数：时长 * 语速，上下浮动一点
        target_len = int(duration * TARGET_SPEED)
        
        context_data.append({
            "id": item['idx'],
            "原文": item['text'],
            "时长": f"{duration:.1f}秒",
            "限制": f"{target_len}字左右"  # 显式告诉LLM字数限制
        })

    events_str = json.dumps(context_data, ensure_ascii=False, indent=2)
    
    return f"""
你现在是CS2知名主播“玩机器”（Machine）。
请将以下【解说原文】重写为【玩机器直播风格】。

【⚠️⚠️ 核心要求：语速控制 ⚠️⚠️】
1. **严格遵守字数限制**：每条数据都标注了`限制`（基于3.5字/秒计算）。
   - 如果时长只有 2秒，你只能说 6-7 个字！(例如："这波donk直接秒了！")
   - 绝不要写长！解说必须跟得上画面！
   - 如果原文很长但时间很短，**必须大幅删减**，只留最核心的击杀信息。

【人设风格】
1. **口语化/造梗**：
   - 拒绝机械播报！不要说"玩家A击杀了玩家B"，要说"A这波定位太准了"、"B直接白给"。
   - 常用词：干拉、白给、这波、有点东西、也是没谁了、这把没了。
   - 称呼：ZywOo=载物, s1mple=森破, NiKo=尼公子, donk=东雪, m0NESY=小孩。
2. **情绪**：
   - 看到连杀要激动：“卧槽！这也能杀？！”
   - 看到失误要吐槽：“这波他在干嘛？他在马什么？”

【输入数据】
{events_str}

【输出格式】
请返回一个 JSON 对象，Key是id，Value是重写后的文本。
例如：
{{
  "0": "这波载物大狙架得太死，没人能过！",
  "1": "donk这就直接干拉了？太自信了吧！"
}}
"""

def process_batch(client, batch_df):
    """处理单个批次"""
    # 准备数据，包含索引以便对应
    batch_input = []
    for idx, row in batch_df.iterrows():
        text = str(row.get('解说文本', ''))
        if not text or text.lower() == 'nan': continue
        
        duration = parse_duration(row.get('时间范围', '0-0s'))
        
        batch_input.append({
            "idx": idx,
            "text": text,
            "duration": duration
        })
    
    if not batch_input: return {}

    prompt = get_machine_style_prompt(batch_input)
    
    try:
        resp = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": "你是一个严格控制语速的CS2解说。请直接输出JSON，不要包含markdown标记。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.8, # 稍微高一点，增加风格化
            response_format={"type": "json_object"}
        )
        content = resp.choices[0].message.content
        # 清洗可能的 markdown
        if content.startswith("```json"): content = content[7:-3]
        
        return json.loads(content)
    except Exception as e:
        print(f"   ⚠️ 批次处理失败: {e}")
        # 失败兜底：返回原文
        return {item['idx']: item['text'] for item in batch_input}

def process_file(csv_path):
    if not os.path.exists(csv_path):
        print(f"❌ 文件不存在: {csv_path}")
        return

    print(f"✨ [Style] 正在玩机器化 (3.5字/s): {os.path.basename(csv_path)}")
    df = pd.read_csv(csv_path)
    
    if '解说文本' not in df.columns:
        print("   ⚠️ 缺少'解说文本'列，跳过")
        return

    # 初始化客户端
    if not MY_API_KEY:
        print("   ❌ 无 API Key")
        return
    client = OpenAI(
    api_key=MY_API_KEY, 
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"
)

    # 分批处理
    results_map = {}
    batches = [df.iloc[i:i+BATCH_SIZE] for i in range(0, len(df), BATCH_SIZE)]
    
    print(f"   🚀 共 {len(df)} 条解说，分为 {len(batches)} 个批次并发处理...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_batch = {executor.submit(process_batch, client, batch): i for i, batch in enumerate(batches)}
        
        completed = 0
        for future in concurrent.futures.as_completed(future_to_batch):
            try:
                batch_res = future.result()
                # 将结果合并到总字典中 (Key是索引, Value是文本)
                # 注意：JSON key 是字符串，需要转回 int 索引
                for k, v in batch_res.items():
                    results_map[int(k)] = v
                
                completed += 1
                if completed % 5 == 0:
                    print(f"      进度: {completed}/{len(batches)}")
            except Exception as e:
                print(f"      ❌ 批次异常: {e}")

    # 回填结果
    # 创建新列，如果没有结果则保留原文本
    new_texts = []
    for idx in df.index:
        if idx in results_map:
            new_texts.append(results_map[idx])
        else:
            new_texts.append(df.at[idx, '解说文本']) # 兜底
            
    df['原解说'] = df['解说文本']
    df['解说文本'] = new_texts
    
    # 保存
    new_path = csv_path.replace(".csv", "_machine_style.csv")
    df.to_csv(new_path, index=False, encoding='utf-8-sig')
    print(f"🎉 润色完成！输出: {new_path}")

if __name__ == "__main__":
    # 自动扫描 output 目录
    base_dir = os.path.join(os.getcwd(), "data")
    target_files = []
    
    if os.path.exists(base_dir):
        # 递归查找 final_schedule.csv
        for root, dirs, files in os.walk(base_dir):
            for file in files:
                if file == "final_schedule.csv":
                    target_files.append(os.path.join(root, file))
    
    if not target_files:
        print("⚠️ 未找到 final_schedule.csv，请先运行主程序。")
    else:
        for f in target_files:
            process_file(f)