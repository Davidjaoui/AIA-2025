import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
import pandas as pd
from datetime import datetime
import random


description = """
Welcome to Jedha Real-time Payments API. This application is part of the Lead program project! Try it out 🕹️
## Endpoints

There is currently just a few endpoints:

* `/`: **GET** request that display a simple default message.
* `/current-transactions`: **GET** request that gives you 1 current transaction

The API is limited to **5 calls/ minutes** 🚧. If you try more, your endpoint will throw back an error.
"""

app = FastAPI(
    title="Jedha - Real-time Payments API 💵",
    description=description,
    version="0.1",
    contact={
        "name": "Jedha",
        "url": "https://jedha.co",
    },
)

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/current-transactions")
@limiter.limit("5/minute")
async def current_transactions(request: Request):
    """
    Return one current transaction in [`.to_json(orient="split")`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_json.html) format in Pandas.
    """
    p = 0.001
    filename = "https://lead-program-assets.s3.eu-west-3.amazonaws.com/M05-Projects/fraudTest.csv"
    df = pd.read_csv(filename, header=0, index_col=[0] ,skiprows=lambda i: i>0 and random.random() > p)
    df = df.sample().loc[:, ~df.columns.isin(["trans_date_trans_time", "fraud", "unix_time"])]
    df["current_time"]= datetime.now()
    return df.to_json(orient="split")

if __name__=="__main__":
    uvicorn.run(app, host="0.0.0.0", port=4000) 