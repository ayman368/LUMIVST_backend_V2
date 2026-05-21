import os
import sys

# Add the backend directory to sys.path so we can import from app
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from app.core.config import settings

def main():
    engine = create_engine(settings.DATABASE_URL, echo=True)
    
    with engine.begin() as conn:
        try:
            conn.execute(
                text("ALTER TABLE wallet_positions ADD COLUMN transactions JSONB NOT NULL DEFAULT '[]'::jsonb")
            )
            print("Successfully added transactions column to wallet_positions")
        except Exception as e:
            if "already exists" in str(e):
                print("Column transactions already exists")
            else:
                print(f"Error adding column: {e}")

if __name__ == "__main__":
    main()
