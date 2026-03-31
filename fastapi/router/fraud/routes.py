from fastapi import APIRouter

from repo.fraud.predict import (
    FraudPredictionRequest,
    FraudPredictionResponse,
    score_fraud,
)

router = APIRouter()


@router.post("/predict", response_model=FraudPredictionResponse)
async def predict_fraud(payload: FraudPredictionRequest) -> FraudPredictionResponse:
    return score_fraud(payload)
