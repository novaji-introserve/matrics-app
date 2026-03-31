# Fraud Model Pipeline

This module implements a safe production path for fraud scoring:

`CSV -> train sklearn model -> export ONNX -> FastAPI loads ONNX for inference`

## Training data

Source CSV path is provided through the `TRAINING_DATA_PATH` environment variable.

In this repository the FastAPI container receives:

`TRAINING_DATA_PATH=/training-data/transaction_training_data.csv`

## Derived fraud label

The source CSV does not contain a dedicated fraud target column.
Training therefore derives the binary label with these agreed rules:

- `1` when `cust_risk_rating >= FRAUD_RISK_RATING_THRESHOLD`
- `1` when `cust_risk_score >= FRAUD_RISK_SCORE_THRESHOLD`
- `0` otherwise

Equivalent expression:

```text
fraud_label = (cust_risk_rating >= FRAUD_RISK_RATING_THRESHOLD) OR (cust_risk_score >= FRAUD_RISK_SCORE_THRESHOLD)
```

In this repository, the defaults in `train.py` are:

- `FRAUD_RISK_RATING_THRESHOLD=3`
- `FRAUD_RISK_SCORE_THRESHOLD=7`

Which means the derived fraud label is:

- fraud when `cust_risk_rating = 3` (implemented as `>= 3`)
- fraud when `cust_risk_score > 6` (implemented as `>= 7`)

## Request fields used by inference

The FastAPI endpoint expects:

- `amount: float`
- `transaction_date: yyyy-mm-dd`
- `currency: str`
- `account_id: int`
- `terminal_id: int`
- `cust_risk_rating: int` in `[1, 3]`
- `cust_risk_score: int` in `[1, 10]`

## Train and export

From the `fastapi/` directory:

```bash
python repo/fraud/train.py
```

From the repository root, using the running FastAPI container:

```bash
docker compose up -d fastapi
docker compose exec fastapi python repo/fraud/train.py
```

When running in Docker, make sure the `fastapi` service has the training-data
directory mounted and `TRAINING_DATA_PATH` set from `fastapi/.env`.

This writes:

- `fastapi/models/fraud/fraud_detector.onnx`
- `fastapi/models/fraud/fraud_detector_metadata.json`

## Serve predictions

FastAPI loads only the saved ONNX model and metadata at inference time.
The API route is:

```text
POST /fraud/predict
```

The response includes:

- `fraud_probability`
- `fraud_score`
- `fraud_prediction`
- `risk_level`
- `model_version`

 Then call the fraud endpoint:

  curl -X POST http://127.0.0.1:8001/fraud/predict \
    -H "Content-Type: application/json" \
    -d '{
      "amount": 25000.0,
      "transaction_date": "2024-03-27",
      "currency": "NGN",
      "account_id": 10668651,
      "terminal_id": 317,
      "cust_risk_rating": 3,
      "cust_risk_score": 2
    }'

  That should come back as fraud-oriented input because cust_risk_rating=3.

  You can also test the score rule directly:

  curl -X POST http://127.0.0.1:8001/fraud/predict \
    -H "Content-Type: application/json" \
    -d '{
      "amount": 25000.0,
      "transaction_date": "2024-03-27",
      "currency": "NGN",
      "account_id": 10668651,
      "terminal_id": 317,
      "cust_risk_rating": 1,
      "cust_risk_score": 7
    }'

  And a likely non-fraud example:

  curl -X POST http://127.0.0.1:8001/fraud/predict \
    -H "Content-Type: application/json" \
    -d '{
      "amount": 25000.0,
      "transaction_date": "2024-03-27",
      "currency": "NGN",
      "account_id": 10668651,
      "terminal_id": 317,
      "cust_risk_rating": 1,
      "cust_risk_score": 2
    }'
