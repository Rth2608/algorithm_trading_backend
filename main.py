import os
import sys
from tqdm import tqdm
from fastapi import FastAPI
from contextlib import asynccontextmanager

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)

from initial_settings.init_crypto_meta import InitCryptoMeta


@asynccontextmanager
async def lifespan(app: FastAPI):
    """DB Update Backend 시작 시 DB 초기화"""
    print("Starting database initialization\n")

    tasks = [InitCryptoMeta]
    for task_class in tqdm(tasks, desc="Initialization Progress", file=sys.stdout, ncols=100, leave=True):
        try:
            task = task_class()
            rows = task.run()
            print(f"{task_class.__name__} completed ({rows} rows)")
        except Exception as e:
            print(f"{task_class.__name__} failed: {e}")
            break

    yield  # 여기서 FastAPI 서버 실행됨

app = FastAPI(title="DB Update Backend", lifespan=lifespan)


@app.get("/")
def root():
    return {"message": "DB Update Backend 실행중"}
