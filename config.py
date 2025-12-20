# config.py：全局配置文件（路径/参数统一管理）
import os

# 1. Demo路径（换Demo只改这里）
DEMO_PATH = r"D:\uncompressed\starladder-budapest-major-2025-spirit-vs-vitality-bo3-IF7bXBRmDsHvo9kSCXua2Z\spirit-vs-vitality-m1-mirage.dem"

# 2. 导航网格路径（从你的日志提取，固定不变）
NAV_DIR = r"C:\Users\Deathwind\.awpy\navs"

# 3. 数据输出路径（自动生成data目录）
OUTPUT_DIR = os.path.join(os.getcwd(), "data")
os.makedirs(OUTPUT_DIR, exist_ok=True)  # 自动创建目录，避免报错
PREPROCESSED_DATA_PATH = os.path.join(OUTPUT_DIR, "preprocessed_data.csv")  # 预处理数据输出
TACTICAL_RESULT_PATH = os.path.join(OUTPUT_DIR, "tactical_result.csv")        # 最终战术结果输出

# 4. 通用参数（调整逻辑只改这里）
TICKRATE = 128  # CS2默认tickrate
INIT_SECOND_RANGE = 2  # 初始站位：回合开始后N秒内（之前用的2秒）
WEIGHT_TYPE = "dist"  # 导航路径权重（文档支持：None/"size"/"dist"）
GRENADE_DURATIONS = {
    "smoke": 18,      # 烟雾持续18秒
    "molotov": 7,     # 燃烧弹/火瓶持续约7秒
    "incendiary": 7,
    "flashbang": 3,   # 闪光弹影响窗口
    "hegrenade": 1    # 手雷瞬时伤害
}