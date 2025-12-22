import os
import pandas as pd
from eco_and_round import get_events_df
from master_scheduler import MasterScheduler

# 1. 填入你的 API KEY
MY_API_KEY = "sk-c2435a4ac2574b4e8ef61ef0c3da7ed4"
# 2. 你的 Demo 路径
DEMO_PATH = r"D:\uncompressed\starladder-budapest-major-2025-spirit-vs-vitality-bo3-IF7bXBRmDsHvo9kSCXua2Z\spirit-vs-vitality-m1-mirage.dem"

def main():
    print("💰 正在专项修复经济解说模块...")
    
    # 强制设置环境变量，确保 eco_and_round 能读到
    os.environ["DASHSCOPE_API_KEY"] = MY_API_KEY
    
    # 删除那个空的缓存文件
    base_name = os.path.splitext(os.path.basename(DEMO_PATH))[0]
    cache_path = os.path.join("data", base_name, "economy_gen_cache.csv")
    if os.path.exists(cache_path):
        os.remove(cache_path)
        print(f"🗑️ 已清除旧的空缓存: {cache_path}")

    # 调用经济分析函数
    # 注意：enable_llm=True 才会调用大模型
    df_eco = get_events_df(DEMO_PATH, enable_llm=True)
    
    if not df_eco.empty:
        print(f"✅ 成功生成 {len(df_eco)} 条经济解说！")
        print("\n现在你可以重新运行 main.py，经济信息就会合并进去了。")
    else:
        print("❌ 经济模块依然未返回数据，请检查网络或 API Key 余额。")

if __name__ == "__main__":
    main()