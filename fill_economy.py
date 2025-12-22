import os
import pandas as pd
from dotenv import load_dotenv
from eco_and_round import get_events_df

# 加载环境变量
load_dotenv()
MY_API_KEY = os.getenv("DASHSCOPE_API_KEY")

# 你的 Demo 路径 (可选，通常通过 main 调用)
DEMO_PATH = r"D:\uncompressed\starladder-budapest-major-2025-spirit-vs-vitality-bo3-IF7bXBRmDsHvo9kSCXua2Z\spirit-vs-vitality-m1-mirage.dem"

def main():
    print("💰 正在专项修复经济解说模块...")
    
    if not MY_API_KEY:
        print("❌ 错误：未找到 API Key")
        return

    os.environ["DASHSCOPE_API_KEY"] = MY_API_KEY
    
    base_name = os.path.splitext(os.path.basename(DEMO_PATH))[0]
    cache_path = os.path.join("data", base_name, "economy_gen_cache.csv")
    
    if os.path.exists(cache_path):
        os.remove(cache_path)
        print(f"🗑️ 已清除旧的空缓存: {cache_path}")

    # 调用经济分析函数
    get_events_df(DEMO_PATH, enable_llm=True)

if __name__ == "__main__":
    main()