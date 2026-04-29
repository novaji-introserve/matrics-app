FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir python-dotenv

WORKDIR /app

COPY screen_daemon.py .
COPY run_escalation.py .
COPY aml_profile_rebuild.py .
COPY run_forever.sh .
COPY run_all.sh .
RUN chmod +x run_forever.sh run_all.sh

ENTRYPOINT ["/app/run_forever.sh"]
