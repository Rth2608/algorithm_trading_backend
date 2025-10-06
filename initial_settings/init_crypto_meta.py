import os
import pandas as pd
from sqlalchemy import text
from tqdm import tqdm
from db_module.connect_sqlalchemy_engine import get_sync_engine


class InitCryptoMeta:
    """crypto_meta 테이블 초기화 및 로그 기록"""

    def __init__(self):
        self.engine = get_sync_engine()
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.csv_path = os.path.join(self.base_dir, "symbol_data", "all_usdt_symbols.csv")

    def run(self):
        """CSV 파일을 읽어 crypto_meta 테이블 초기화"""
        with self.engine.begin() as conn:
            # init_logs 테이블 생성 (백엔드 초기 시작 로그 저장)
            conn.execute(text("""
                CREATE SCHEMA IF NOT EXISTS log_data;
                CREATE TABLE IF NOT EXISTS log_data.init_logs (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT NOW(),
                    action TEXT,
                    detail TEXT
                );
            """))
            conn.execute(text("""
                INSERT INTO log_data.init_logs (action, detail)
                VALUES ('start', 'Initializing crypto_meta table');
            """))

            # crypto_meta 테이블 생성 (코인의 기본정보 저장할 테이블)
            conn.execute(text("""
                CREATE SCHEMA IF NOT EXISTS crypto_info;
                CREATE TABLE IF NOT EXISTS crypto_info.crypto_meta (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT UNIQUE NOT NULL
                );
            """))

            # CSV 존재 확인
            if not os.path.exists(self.csv_path):
                raise FileNotFoundError(f"CSV file not found: {self.csv_path}")

            # CSV 로드
            df = pd.read_csv(self.csv_path)
            if "symbol" not in df.columns:
                raise ValueError("'symbol' column not found in the CSV file.")
            
            # 중복 제거
            df = df.drop_duplicates(subset=["symbol"]).reset_index(drop=True)

            # 데이터 삽입
            conn.execute(text("TRUNCATE TABLE crypto_info.crypto_meta;"))
            for _, row in tqdm(df.iterrows(), total=len(df), desc="Save symbols data"):
                conn.execute(text("""
                    INSERT INTO crypto_info.crypto_meta (symbol)
                    VALUES (:symbol)
                    ON CONFLICT (symbol) DO NOTHING;
                """), {"symbol": row["symbol"]})

            # 완료 로그
            conn.execute(text("""
                INSERT INTO log_data.init_logs (action, detail)
                VALUES ('complete', 'crypto_meta table initialization done');
            """))

            return len(df)
