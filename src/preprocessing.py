import pandas as pd
import re
import json
from pathlib import Path

# fungsi load data mentah
def load_json_to_df(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    records = []
    for t in data:
        record = {
            "id": t.get("id"),
            "author_id": t.get("author_id"),
            "created_at": t.get("created_at"),
            "text": t.get("text"),
            "like_count": t["public_metrics"]["like_count"],
            "retweet_count": t["public_metrics"]["retweet_count"],
            "reply_count": t["public_metrics"]["reply_count"],
            "quote_count": t["public_metrics"]["quote_count"],
        }
        records.append(record)
    return pd.DataFrame(records)

# kamus normalisasi simpel
normalisasi_dict = {
    " gak ": "tidak",
    " ga ": "tidak",
    " nggak ": "tidak",
    " udah ": "sudah",
    " udh ": "sudah",
    " aja ": "saja",
    " pake ": "pakai",
    " bgt ": "banget",
    " yg ": "yang",
    " kalo ": "kalau",
    " sm ": "sama",
    " dr ": "dari",
    " tp ": "tapi",
    " dg ": "dengan",
    " krn ": "karena",
    " dlm ": "dalam"
}

# fungsi clean data
def clean_text(text):
    if pd.isna(text):
        return ""
    text = re.sub(r"http\S+", "", text)  # hapus URL
    text = re.sub(r"@\w+", "", text)     # hapus mention
    text = re.sub(r"#\w+", "", text)     # hapus hashtag
    text = re.sub(r"[^\w\s]", " ", text) # hapus tanda baca/simbol
    text = re.sub(r"\d+", "", text)      # hapus angka
    text = text.lower()                  # lowercase
    text = re.sub(r"\s+", " ", text).strip()
    return text

# fungsi normalisasi data
def normalize_text(text):
    words = text.split()
    normalized_words = [normalisasi_dict.get(w, w) for w in words]
    return " ".join(normalized_words)

# path ke data mentah
base_dir = Path(__file__).parent.parent
json_path = base_dir / "raw_data" / "raw_data.json"

# load data mentah
df = load_json_to_df(json_path)

print("----------------------Raw Data---------------------")
print(df)

# proses cleaning dan normalisasi
df["text"] = df["text"].apply(clean_text).apply(normalize_text)

print("----------------------Clean Data---------------------")
print(df)

clean_path = base_dir / "clean_data" / "celan_data.csv"
df.to_csv(clean_path, index=False, encoding="utf-8")

print('data tersimpan')