FROM jedha/real-time-payments-api-lead-program

COPY . .

CMD gunicorn app:app  --bind 0.0.0.0:$PORT --worker-class uvicorn.workers.UvicornWorker