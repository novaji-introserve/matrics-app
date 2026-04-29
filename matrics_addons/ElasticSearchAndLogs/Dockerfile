FROM python:3.10-slim

RUN pip install --no-cache-dir elasticsearch python-dotenv

ENV PYTHONUNBUFFERED=1

USER root

CMD ["python3", "/mnt/ElasticSearchAndLogs/logger.py"]
