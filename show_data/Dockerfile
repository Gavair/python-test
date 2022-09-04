FROM python:3.10

WORKDIR /app

COPY . /app

RUN pip install -r /app/requirements.txt

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "5001"]
