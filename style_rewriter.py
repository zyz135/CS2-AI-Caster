import pandas as pd
import os
import json
import glob
import concurrent.futures
from openai import OpenAI
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# ================= 配置区域 =================
MY_API_KEY = os.getenv("DASHSCOPE_API_KEY") 
MODEL_NAME = "qwen3-max" 
BLACKLIST_KEYWORDS = [
    "摔死", "自杀", "未知", "world", "World", "Trigger", "entity", "Bot", "BOT"
]
# ===========================================

def get_machine_style_prompt_with_timing(events_batch):
    events_str = json.dumps(events_batch, ensure_ascii=False)
    
    return f"""
你现在是CS2知名主播“玩机器”（Machine）。
请将以下【解说原文】重写为【玩机器直播风格】，并**严格遵守时长限制**。

【人设要求】
1. **风格**：极度口语化、阴阳怪气、造梗大师。
   - 常用词：白给、这就给啦？、顶级理解、只有干拉、没什么好说的、也就是个donk。
   - 称呼：FlameZ=火仔, ZywOo=载物/薯薯, donk=小孩/那小孩, sh1ro=虽然, mezii=梅西。
2. **拒绝电报风**：虽然有时长限制，但不要写成“T进攻A”，要说“T这就直接干拉A区了啊”。
   - 宁可语速快一点（字数稍多），也不要说话只说一半！
3. **枪械黑话**：USP=小手枪, Glock=格洛克/滋水枪, AWP=大狙, SSG08=鸟狙, Deagle=沙鹰。
4. **过滤**：遇到“摔死/自杀”直接返回空字符串 ""。

【输入数据】(JSON):
{events_str}

【输出格式】(纯JSON列表，只包含重写后的文本字符串，顺序对应):
["重写后文本1", "重写后文本2", ""]
"""

def process_batch(client, batch_df):
    events_batch = []
    for _, row in batch_df.iterrows():
        # 估算时长
        t_range = str(row['时间范围']).replace('s', '').split('-')
        duration = 4.0
        try: duration = float(t_range[1]) - float(t_range[0])
        except: pass
        
        events_batch.append({
            "text": row['解说文本'],
            "duration": round(duration, 1)
        })
        
    prompt = get_machine_style_prompt_with_timing(events_batch)
    
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.8
        )
        content = response.choices[0].message.content
        # 清洗 JSON
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0]
        elif "```" in content:
            content = content.split("```")[1].split("```")[0]
            
        result_list = json.loads(content.strip())
        if len(result_list) != len(batch_df):
            return batch_df['解说文本'].tolist()
        return result_list
    except Exception as e:
        print(f"⚠️ 批次处理失败: {e}")
        return batch_df['解说文本'].tolist()

def main():
    if not MY_API_KEY:
        print("❌ 错误：未找到 API Key，请检查 .env 文件")
        return

    client = OpenAI(api_key=MY_API_KEY, base_url="https://dashscope.aliyuncs.com/compatible-mode/v1")
    
    # 扫描生成的 final csv
    files = glob.glob(os.path.join("data", "**", "final_*_half.csv"), recursive=True)
    if not files:
        print("❌ 未找到 final_upper/lower_half.csv，请先运行主程序。")
        return

    for filepath in files:
        print(f"🎨 正在润色: {os.path.basename(filepath)} ...")
        try:
            df = pd.read_csv(filepath, encoding='utf-8-sig')
            
            # 分批处理
            BATCH_SIZE = 10
            batches = [df[i:i + BATCH_SIZE] for i in range(0, len(df), BATCH_SIZE)]
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

if __name__ == "__main__":
    main()