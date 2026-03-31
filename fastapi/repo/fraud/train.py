import csv
import json
import os
from collections import Counter
from datetime import datetime
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


MODEL_DIR = Path(__file__).resolve().parents[2] / "models" / "fraud"
MODEL_PATH = MODEL_DIR / "fraud_detector.onnx"
METADATA_PATH = MODEL_DIR / "fraud_detector_metadata.json"


def _get_thresholds() -> tuple[int, int]:
    return (
        int(os.getenv("FRAUD_RISK_RATING_THRESHOLD", "3")),
        int(os.getenv("FRAUD_RISK_SCORE_THRESHOLD", "7")),
    )


def get_csv_path() -> Path:
    raw_path = os.getenv("TRAINING_DATA_PATH", "").strip()
    if not raw_path:
        raise RuntimeError(
            "TRAINING_DATA_PATH is not set. Define it in fastapi/.env and expose it in docker-compose.yml."
        )
    return Path(raw_path)


def _build_currency_map(rows: list[dict[str, str]]) -> dict[str, int]:
    currencies = sorted({(row.get("currency") or "").strip().upper() for row in rows if row.get("currency")})
    return {currency: index for index, currency in enumerate(currencies)}


def _parse_date(value: str) -> tuple[int, int, int, int]:
    parsed = datetime.strptime(value, "%Y-%m-%d").date()
    return parsed.year, parsed.month, parsed.day, parsed.weekday()


def _derive_target(row: dict[str, str]) -> int:
    # The source CSV has no explicit fraud label, so training derives one from
    # the agreed risk thresholds:
    # - fraud if cust_risk_rating >= FRAUD_RISK_RATING_THRESHOLD
    # - fraud if cust_risk_score >= FRAUD_RISK_SCORE_THRESHOLD
    rating_threshold, score_threshold = _get_thresholds()
    rating = int(row["cust_risk_rating"])
    score = int(row["cust_risk_score"])
    return int(rating >= rating_threshold or score >= score_threshold)


def _feature_vector(row: dict[str, str], currency_map: dict[str, int]) -> list[float]:
    year, month, day, weekday = _parse_date(row["tran_date"])
    currency = (row.get("currency") or "").strip().upper()
    return [
        float(row["amt"]),
        float(year),
        float(month),
        float(day),
        float(weekday),
        float(currency_map.get(currency, -1)),
        float(int(row["acnt_id"])),
        float(int(row["terminal_id"])),
        float(int(row["cust_risk_rating"])),
        float(int(row["cust_risk_score"])),
    ]


def load_training_data(csv_path: Path) -> tuple[np.ndarray, np.ndarray, dict[str, int], dict[str, int]]:
    with csv_path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))

    currency_map = _build_currency_map(rows)
    features = np.asarray([_feature_vector(row, currency_map) for row in rows], dtype=np.float32)
    targets = np.asarray([_derive_target(row) for row in rows], dtype=np.int64)
    label_distribution = dict(Counter(int(value) for value in targets))
    return features, targets, currency_map, label_distribution


def train_and_export() -> None:
    csv_path = get_csv_path()
    if not csv_path.exists():
        raise FileNotFoundError(f"Training CSV not found at {csv_path}")

    try:
        from skl2onnx import convert_sklearn
        from skl2onnx.common.data_types import FloatTensorType
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "skl2onnx is not installed. Install fastapi requirements before training the fraud model."
        ) from exc

    features, targets, currency_map, label_distribution = load_training_data(csv_path)
    if len(label_distribution) < 2:
        raise RuntimeError(
            "Training data produced a single class only. "
            f"Label distribution: {label_distribution}. "
            "Adjust the fraud thresholds or provide training data with both classes."
        )

    rating_threshold, score_threshold = _get_thresholds()

    x_train, x_test, y_train, y_test = train_test_split(
        features,
        targets,
        test_size=0.2,
        random_state=42,
        stratify=targets,
    )

    pipeline = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            ("classifier", LogisticRegression(max_iter=1000, class_weight="balanced")),
        ]
    )
    pipeline.fit(x_train, y_train)

    accuracy = float(pipeline.score(x_test, y_test))

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    onnx_model = convert_sklearn(
        pipeline,
        initial_types=[("features", FloatTensorType([None, x_train.shape[1]]))],
        options={id(pipeline.named_steps["classifier"]): {"zipmap": False}},
        target_opset=17,
    )
    MODEL_PATH.write_bytes(onnx_model.SerializeToString())

    metadata = {
        "model_version": datetime.utcnow().strftime("%Y%m%d%H%M%S"),
        "source_csv": str(csv_path),
        "feature_order": [
            "amount",
            "transaction_year",
            "transaction_month",
            "transaction_day",
            "transaction_weekday",
            "currency_code",
            "account_id",
            "terminal_id",
            "cust_risk_rating",
            "cust_risk_score",
        ],
        "currency_map": currency_map,
        "label_strategy": {
            "derived_target": 1,
            "rules": [
                f"cust_risk_rating >= {rating_threshold}",
                f"cust_risk_score >= {score_threshold}",
            ],
            "expression": (
                "cust_risk_rating >= "
                f"{rating_threshold} or cust_risk_score >= {score_threshold}"
            ),
            "else_target": 0,
        },
        "class_labels": [0, 1],
        "test_accuracy": round(accuracy, 6),
        "row_count": int(features.shape[0]),
        "label_distribution": label_distribution,
        "trained_at_utc": datetime.utcnow().isoformat() + "Z",
    }
    METADATA_PATH.write_text(json.dumps(metadata, indent=2, sort_keys=True))

    print(f"Saved ONNX model to {MODEL_PATH}")
    print(f"Saved metadata to {METADATA_PATH}")
    print(f"Holdout accuracy: {accuracy:.4f}")
    print(f"Label distribution: {label_distribution}")


if __name__ == "__main__":
    train_and_export()
