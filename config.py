# config.py：全局配置文件
import os

# 1. Demo路径
DEMO_PATH = r"D:\uncompressed\starladder-budapest-major-2025-spirit-vs-vitality-bo3-IF7bXBRmDsHvo9kSCXua2Z\spirit-vs-vitality-m1-mirage.dem"

# 2. 导航网格路径
NAV_DIR = r"C:\Users\Deathwind\.awpy\navs"

# 3. 数据输出路径
OUTPUT_DIR = os.path.join(os.getcwd(), "data")
os.makedirs(OUTPUT_DIR, exist_ok=True)
PREPROCESSED_DATA_PATH = os.path.join(OUTPUT_DIR, "preprocessed_data.csv")
TACTICAL_RESULT_PATH = os.path.join(OUTPUT_DIR, "tactical_result.csv")

# 4. 通用参数
# 🔥🔥🔥 核心修改：强制改为 64 🔥🔥🔥
TICKRATE = 64  
INIT_SECOND_RANGE = 2 
WEIGHT_TYPE = "dist"
GRENADE_DURATIONS = {
    "smoke": 18,
    "molotov": 7,
    "incendiary": 7,
    "flashbang": 3,
    "hegrenade": 1
}