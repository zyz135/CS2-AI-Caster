import os
import argparse
import subprocess
import sys
from dotenv import load_dotenv 
from master_scheduler import MasterScheduler

# 加载 .env 文件中的变量
load_dotenv()

# 从环境变量获取 Key
MY_API_KEY = os.getenv("DASHSCOPE_API_KEY")

def run_script(script_name, test_mode=False):
    """辅助函数：运行外部脚本"""
    if not os.path.exists(script_name):
        print(f"⚠️ [Skipped] 找不到脚本: {script_name}，跳过该步骤。")
        return
    
    print(f"\n🚀 [Auto-Runner] 正在执行: {script_name} ...")
    # 使用当前解释器运行脚本
    # 如果是 style_rewriter.py 且测试模式，通过环境变量传递
    env = os.environ.copy()
    if script_name == "style_rewriter.py" and test_mode:
        env["TEST_MODE"] = "1"
    
    result = subprocess.run([sys.executable, script_name], capture_output=False, env=env)
    
    if result.returncode != 0:
        print(f"❌ [Error] {script_name} 运行出错！")
    else:
        print(f"✅ [Success] {script_name} 执行完毕。")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--demo", type=str, required=True, help="Demo文件路径")
    parser.add_argument("--test", action="store_true", help="测试模式：只生成第一回合的文本")
    args = parser.parse_args()

    if not os.path.exists(args.demo):
        print(f"❌ 找不到文件: {args.demo}")
        return

    if not MY_API_KEY:
        print("❌ 错误：未找到 API Key！请确保项目根目录下有 .env 文件并配置了 DASHSCOPE_API_KEY")
        return

    # ==========================================
    # 第一步：数据清洗 (Clean Module)
    # ==========================================
    run_script("clean_cache.py", test_mode=args.test)

    # ==========================================
    # 第二步：核心调度与生成 (Master Scheduler)
    # ==========================================
    print("\n⚔️ [Master] 开始运行主调度器 (v3.3 固定64Tick版)...")
    try:
        # 设置环境变量，确保子模块能读到 Key
        os.environ["DASHSCOPE_API_KEY"] = MY_API_KEY
        os.environ["OPENAI_API_KEY"] = MY_API_KEY
        
        # 实例化并运行
        scheduler = MasterScheduler(args.demo, MY_API_KEY, test_mode=args.test)
        scheduler.run()
        
    except Exception as e:
        print(f"❌ 运行调度器出错: {e}")
        import traceback
        traceback.print_exc()
        return 

    # ==========================================
    # 第三步：风格润色 (Style Rewriter)
    # ==========================================
    run_script("style_rewriter.py", test_mode=args.test)

    print("\n🎉🎉🎉 全流程执行完毕！可以直接去 data 文件夹看结果了！")

if __name__ == "__main__":
    main()