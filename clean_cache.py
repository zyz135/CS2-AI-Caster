import pandas as pd
import os
import glob

def clean_text(text):
    if pd.isna(text): return ""
    txt = str(text)
    # 脏词黑名单
    tags = ["短版", "中版", "长版", "Short", "Medium", "Long", ":", "：", "version"]
    for tag in tags:
        txt = txt.replace(tag, "")
    return txt.replace("---", "").strip()

def main():
    # 扫描所有缓存文件
    files = glob.glob(os.path.join("data", "**", "*_gen_cache.csv"), recursive=True)
    if not files:
        print("❌ 没找到缓存文件")
        return

    print(f"🔍 找到 {len(files)} 个文件，开始清洗...")
    for f in files:
        try:
            df = pd.read_csv(f, encoding='utf-8-sig')
            cols = ['short_text_neutral', 'medium_text_neutral', 'long_text_neutral']
            for c in cols:
                if c in df.columns:
                    df[c] = df[c].apply(clean_text) # 洗刷刷
            df.to_csv(f, index=False, encoding='utf-8-sig')
            print(f"   ✅ 已清洗: {os.path.basename(f)}")
        except Exception as e:
            print(f"   ❌ 失败 {f}: {e}")

if __name__ == "__main__":
    main()