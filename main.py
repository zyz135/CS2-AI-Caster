import os
import argparse
import sys
import traceback

try:
    from pretreatment import extract_specified_player_data_wrapper
    # 导入新的主分析函数
    from data_analysis import run_tactical_analysis
except ImportError as e:
    print(f"❌ 导入错误: {e}")
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="CS2 战术解说生成器")
    parser.add_argument("--demo", type=str, required=True, help="Demo文件路径")
    parser.add_argument("--force", action="store_true", help="强制重新提取原始数据")
    parser.add_argument("--test", action="store_true", help="测试模式 (仅跑 R1 和 R13)")
    args = parser.parse_args()

    if not os.path.exists(args.demo):
        print(f"❌ Demo文件不存在: {args.demo}")
        return

    # 路径设置
    base_name = os.path.splitext(os.path.basename(args.demo))[0]
    output_dir = os.path.join("data", base_name)
    os.makedirs(output_dir, exist_ok=True)
    path_raw_csv = os.path.join(output_dir, "1_raw_data.csv")

    print("="*60)
    print(f"🎬 任务: {base_name}")
    print("="*60)

    # === Step 1: 数据提取 ===
    if args.force or not os.path.exists(path_raw_csv):
        print(f"\n[1/2] 正在提取数据...")
        try:
            extract_specified_player_data_wrapper(args.demo, path_raw_csv)
        except Exception as e:
            print(f"❌ 提取失败: {e}")
            traceback.print_exc()
            return
    else:
        print(f"\n⚡ [1/2] 原始数据已存在，跳过提取。")

    # === Step 2: 战术分析 ===
    print(f"\n[2/2] 正在进行战术分析 (多版本文本生成)...")
    
    # 根据参数决定跑哪些回合
    target_rounds = [1, 13] if args.test else None
    
    try:
        run_tactical_analysis(path_raw_csv, output_dir, target_rounds=target_rounds)
    except Exception as e:
        print(f"❌ 分析失败: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()