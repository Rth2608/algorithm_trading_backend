from fastapi import FastAPI
from routers import auth_google, admin, ohlcv_rest
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="DB backend")


origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_google.router)
app.include_router(admin.router)
app.include_router(ohlcv_rest.router)


@app.get("/")
def root():
    return {"msg": "DB backend"}
