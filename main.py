import os
import argparse
import subprocess
import sys
from master_scheduler import MasterScheduler

# === 填入你的 Key ===
MY_API_KEY = "sk-c2435a4ac2574b4e8ef61ef0c3da7ed4"

def run_script(script_name):
    """辅助函数：运行外部脚本"""
    if not os.path.exists(script_name):
        print(f"⚠️ [Skipped] 找不到脚本: {script_name}，跳过该步骤。")
        return
    
    print(f"\n🚀 [Auto-Runner] 正在执行: {script_name} ...")
    # 使用当前解释器运行脚本
    result = subprocess.run([sys.executable, script_name], capture_output=False)
    
    if result.returncode != 0:
        print(f"❌ [Error] {script_name} 运行出错！")
    else:
        print(f"✅ [Success] {script_name} 执行完毕。")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--demo", type=str, required=True, help="Demo文件路径")
    args = parser.parse_args()

    if not os.path.exists(args.demo):
        print(f"❌ 找不到文件: {args.demo}")
        return

    # ==========================================
    # 第一步：数据清洗 (Clean Module)
    # ==========================================
    # 使用你提供的 clean_cache.py 清除格式杂质
    # (由于 MasterScheduler 现在优先读 Medium/Long，我们不需要填充 Short 了，只要洗干净就行)
    run_script("clean_cache.py")

    # ==========================================
    # 第二步：核心调度与生成 (Master Scheduler)
    # ==========================================
    print("\n⚔️ [Master] 开始运行主调度器 (v3.1 中长文本优先版)...")
    try:
        # 设置环境变量，确保子模块能读到 Key
        os.environ["DASHSCOPE_API_KEY"] = MY_API_KEY
        os.environ["OPENAI_API_KEY"] = MY_API_KEY
        
        # 实例化并运行
        scheduler = MasterScheduler(args.demo, MY_API_KEY)
        scheduler.run()
        
    except Exception as e:
        print(f"❌ 运行调度器出错: {e}")
        import traceback
        traceback.print_exc()
        return # 如果调度器挂了，就不跑润色了

    # ==========================================
    # 第三步：风格润色 (Style Rewriter)
    # ==========================================
    # 把生成的 final_xxx.csv 变成“玩机器”风格
    run_script("style_rewriter.py")

    print("\n🎉🎉🎉 全流程执行完毕！可以直接去 data 文件夹看结果了！")

if __name__ == "__main__":
    main()