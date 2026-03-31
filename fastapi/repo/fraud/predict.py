import json
from datetime import date
from functools import lru_cache
from pathlib import Path

import numpy as np
from pydantic import BaseModel, ConfigDict, Field


MODEL_DIR = Path(__file__).resolve().parents[2] / "models" / "fraud"
MODEL_PATH = MODEL_DIR / "fraud_detector.onnx"
METADATA_PATH = MODEL_DIR / "fraud_detector_metadata.json"


class FraudPredictionRequest(BaseModel):
    amount: float
    transaction_date: date
    currency: str
    account_id: int
    terminal_id: int
    cust_risk_rating: int = Field(ge=1, le=3)
    cust_risk_score: int = Field(ge=1, le=10)

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "amount": 25000.0,
                "transaction_date": "2024-03-27",
                "currency": "NGN",
                "account_id": 10668651,
                "terminal_id": 317,
                "cust_risk_rating": 1,
                "cust_risk_score": 3,
            }
        }
    )


class FraudPredictionResponse(BaseModel):
    fraud_probability: float = Field(ge=0, le=1)
    fraud_score: int = Field(ge=0, le=100)
    fraud_prediction: bool
    risk_level: str
    model_version: str


class OnnxFraudPredictor:
    def __init__(self, model_path: Path, metadata_path: Path):
        try:
            import onnxruntime as ort
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "onnxruntime is not installed. Add it to the FastAPI environment before using /fraud/predict."
            ) from exc

        if not model_path.exists():
            raise RuntimeError(
                f"Fraud ONNX model not found at {model_path}. Train it first with fastapi/repo/fraud/train.py."
            )
        if not metadata_path.exists():
            raise RuntimeError(
                f"Fraud model metadata not found at {metadata_path}. Train it first with fastapi/repo/fraud/train.py."
            )

        self.model_path = model_path
        self.metadata_path = metadata_path
        self.metadata = json.loads(metadata_path.read_text())
        self.session = ort.InferenceSession(str(model_path), providers=["CPUExecutionProvider"])
        self.input_name = self.session.get_inputs()[0].name
        self.output_names = [output.name for output in self.session.get_outputs()]

    def _currency_code(self, currency: str) -> int:
        currency_map = self.metadata.get("currency_map", {})
        return int(currency_map.get(currency.strip().upper(), -1))

    def _feature_vector(self, payload: FraudPredictionRequest) -> np.ndarray:
        tx_date = payload.transaction_date
        features = [
            float(payload.amount),
            float(tx_date.year),
            float(tx_date.month),
            float(tx_date.day),
            float(tx_date.weekday()),
            float(self._currency_code(payload.currency)),
            float(payload.account_id),
            float(payload.terminal_id),
            float(payload.cust_risk_rating),
            float(payload.cust_risk_score),
        ]
        return np.asarray([features], dtype=np.float32)

    def predict(self, payload: FraudPredictionRequest) -> FraudPredictionResponse:
        features = self._feature_vector(payload)
        outputs = self.session.run(self.output_names, {self.input_name: features})

        predicted_label = int(np.asarray(outputs[0]).reshape(-1)[0])
        probabilities = np.asarray(outputs[1])
        fraud_probability = float(probabilities.reshape(1, -1)[0][1])
        fraud_score = int(round(fraud_probability * 100))
        risk_level = _risk_level(fraud_probability)

        return FraudPredictionResponse(
            fraud_probability=round(fraud_probability, 6),
            fraud_score=fraud_score,
            fraud_prediction=bool(predicted_label),
            risk_level=risk_level,
            model_version=str(self.metadata.get("model_version", "unknown")),
        )


def _risk_level(probability: float) -> str:
    if probability >= 0.7:
        return "high"
    if probability >= 0.4:
        return "medium"
    return "low"


@lru_cache(maxsize=1)
def get_fraud_predictor() -> OnnxFraudPredictor:
    return OnnxFraudPredictor(MODEL_PATH, METADATA_PATH)


def score_fraud(payload: FraudPredictionRequest) -> FraudPredictionResponse:
    return get_fraud_predictor().predict(payload)
