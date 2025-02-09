from fastapi import FastAPI
from transformers import pipeline
from pydantic import BaseModel
from typing import List

model_name = "tabularisai/multilingual-sentiment-analysis"
sentiment_pipeline = pipeline("text-classification", model=model_name)


class SentimentRequest(BaseModel):
    texts: List[str]


app = FastAPI()


@app.get("/")
def read_root():
    return {"message": "Sentiment Analysis API is running!"}


@app.post("/predict/")
def predict(request: SentimentRequest):
    print(f"predicting, n={len(request.texts)}")
    results = sentiment_pipeline(request.texts, padding=True)
    return {
        "labels": [r["label"] for r in results],
    }
