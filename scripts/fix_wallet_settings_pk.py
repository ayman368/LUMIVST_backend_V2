import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not found in .env")
    sys.exit(1)

def fix_pk():
    print("Fixing Primary Key for wallet_settings...")
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE wallet_settings DROP CONSTRAINT IF EXISTS wallet_settings_pkey CASCADE;"))
            conn.execute(text("ALTER TABLE wallet_settings ADD PRIMARY KEY (key, user_id);"))
            conn.commit()
            print("Successfully updated Primary Key to (key, user_id)!")
        except Exception as e:
            print(f"Error: {e}")
            conn.rollback()

if __name__ == "__main__":
    fix_pk()
