"""Milestone 1: train the offline fraud model on FraudBench and export artifacts.

FraudBench (TristanYan/FraudBench) is a refund/damage-claim fraud benchmark built on Amazon
reviews, not a card-transaction table. We treat each damage claim as one "transaction":
  * fraud (label 1): a DeepFake claim, i.e. a fabricated-damage image edited from a Positive
    (undamaged, 5-star) purchase; its purchase-side fields come from that source review;
  * legit (label 0): a genuine Negative damage claim.
Only purchase-side features known at claim time are used. Review text, rating, title and
helpful votes are excluded: they are label proxies (fraud rows all come from 4-5 star reviews)
and would not exist at authorization time.

Outputs: checkpoints/model.txt (git-ignored), results/model_metrics.md, results/model_metrics.json,
results/golden_scores.json (200 held-out rows + Python scores for the Go parity test).
"""
import glob
import json
import os
from datetime import datetime, timezone

import lightgbm as lgb
import numpy as np
import pandas as pd
from huggingface_hub import snapshot_download
from sklearn.metrics import roc_auc_score

os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")

SEED = 7
TEST_FRAC = 0.4  # large enough that the held-out split has >= 200 rows for the golden export
RATES = (0.005, 0.01, 0.02)
FEATURES = ["price", "avg_rating", "rating_number", "verified_purchase", "n_images",
            "n_categories", "n_features", "hour", "dow", "category_id"]
PARAMS = dict(objective="binary", learning_rate=0.05, num_leaves=15, min_data_in_leaf=10,
              feature_fraction=0.8, bagging_fraction=0.8, bagging_freq=1, lambda_l2=1.0,
              seed=SEED, deterministic=True, num_threads=4, verbose=-1)
ROUNDS = 300


def load_dir(root: str) -> str:
    """Download only the JSON metadata of FraudBench into data/ (images are not needed)."""
    return snapshot_download("TristanYan/FraudBench", repo_type="dataset",
                             allow_patterns=["*.json", "README.md"], local_dir=root)


def purchase_features(r: dict, category: str) -> dict:
    """Claim-time features from a review record (its purchase and product).

    Missing price stays NaN (LightGBM and leaves both route NaN via the trained default direction)."""
    pm, ts = r.get("product_meta") or {}, datetime.fromtimestamp(r["timestamp"] / 1000, timezone.utc)
    price = pm.get("price")
    return dict(
        price=float(price) if isinstance(price, (int, float)) else np.nan,
        avg_rating=pm.get("average_rating"), rating_number=pm.get("rating_number"),
        verified_purchase=int(bool(r["verified_purchase"])), n_images=len(r.get("images") or []), category=category,
        n_categories=len(pm.get("categories") or []), n_features=len(pm.get("features") or []),
        hour=ts.hour, dow=ts.weekday(), timestamp=r["timestamp"])


def build_frame(root: str) -> pd.DataFrame:
    """One row per claim: genuine Negative claims (0) and DeepFake claims (1)."""
    rows = []
    dropped = 0  # ~27% of review records are stubs without timestamp/product_meta: unusable
    for f in glob.glob(f"{root}/*/Negative/*/MetaReview_*.json"):
        r = json.load(open(f))
        if "timestamp" not in r or "product_meta" not in r:
            dropped += 1
            continue
        rows.append({**purchase_features(r, f.split("/")[-4]), "label": 0})
    for f in glob.glob(f"{root}/*/DeepFake/Metadata/Edit_*.json"):
        edit = json.load(open(f))
        cat = f.split("/")[-4]  # directory name; the JSON main_category can differ from it
        src = glob.glob(f"{glob.escape(root)}/{glob.escape(cat)}/Positive/{edit['source_review']}/MetaReview_*.json")
        r = json.load(open(src[0]))
        if "timestamp" not in r or "product_meta" not in r:
            dropped += 1
            continue
        rows.append({**purchase_features(r, cat), "label": 1})
    print(f"dropped {dropped} stub records without timestamp/product_meta")
    df = pd.DataFrame(rows)
    df["category_id"] = df["category"].astype("category").cat.codes
    return df.sort_values("timestamp", kind="stable").reset_index(drop=True)


def recall_at_fdr(y, s, rate):
    """Best recall with at most `rate` of legit rows declined (threshold on legit scores)."""
    legit = np.sort(s[y == 0])[::-1]
    k = int(np.floor(rate * len(legit)))          # legit rows allowed above the threshold
    thr = legit[k] if k < len(legit) else -np.inf  # strictly-greater rule keeps FDR <= rate
    flagged = s > thr
    return dict(threshold=float(thr), recall=float(flagged[y == 1].mean()),
                false_decline_rate=float(flagged[y == 0].mean()), false_declines=int(flagged[y == 0].sum()))


def main():
    os.makedirs("checkpoints", exist_ok=True)
    os.makedirs("results", exist_ok=True)
    df = build_frame(load_dir("data/fraudbench"))
    cut = int(len(df) * (1 - TEST_FRAC))
    train, test = df.iloc[:cut], df.iloc[cut:]  # time-ordered: test is strictly later
    booster = lgb.train(PARAMS, lgb.Dataset(train[FEATURES], train["label"]), ROUNDS)
    booster.save_model("checkpoints/model.txt")
    s = booster.predict(test[FEATURES])
    y = test["label"].to_numpy()
    table = {f"{r * 100:g}%": recall_at_fdr(y, s, r) for r in RATES}
    auc = float(roc_auc_score(y, s))

    gold = test.sample(200, random_state=SEED).sort_index()
    gs = booster.predict(gold[FEATURES])
    # NaN is kept as null in JSON; LightGBM treats missing values via the default direction.
    feats = [[None if np.isnan(v) else float(v) for v in row] for row in gold[FEATURES].to_numpy(float)]
    json.dump({"feature_names": FEATURES, "rows": [{"features": f, "score": float(v), "label": int(l)}
              for f, v, l in zip(feats, gs, gold["label"])]}, open("results/golden_scores.json", "w"))

    meta = dict(n_total=len(df), n_train=len(train), n_test=len(test),
                test_fraud=int(y.sum()), test_legit=int((y == 0).sum()), auc=auc,
                train_end=int(train.timestamp.max()), test_start=int(test.timestamp.min()),
                recall_at_false_decline_rate=table)
    json.dump(meta, open("results/model_metrics.json", "w"), indent=2)
    lines = [
        "# Fraud model metrics (FraudBench, time-ordered held-out test split)", "",
        f"Split: claims sorted by timestamp; first {len(train)} train, last {len(test)} test "
        f"({meta['test_fraud']} fraud, {meta['test_legit']} legit). Test AUC {auc:.4f}.",
        "Fraud = DeepFake damage claims, legit = genuine Negative damage claims "
        "(see train.py docstring; FraudBench is a claim-fraud proxy, not card data).", "",
        "Recall at a fixed false-decline rate (threshold chosen on test legit scores, so the "
        "false-decline rate is at most the stated rate; with few legit rows, 0.5% means zero false declines).", "",
        "| false-decline rate | recall | threshold | achieved false-decline rate |", "|---|---|---|---|"]
    for k, v in table.items():
        lines.append(f"| {k} | {v['recall']:.4f} | {v['threshold']:.4f} | {v['false_decline_rate']:.4f} |")
    open("results/model_metrics.md", "w").write("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
