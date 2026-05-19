import os
import sys
import psycopg2
from dotenv import load_dotenv

# Add the project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

def run_wallet_init():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    
    if not db_url:
        print("❌ Error: DATABASE_URL not found in .env")
        return

    print("🚀 Initializing Wallet Database Tables...")
    
    try:
        # Read the SQL schema
        schema_path = os.path.join(os.path.dirname(__file__), "..", "wallet_schema.sql")
        with open(schema_path, "r", encoding="utf-8") as f:
            sql = f.read()

        # Connect and execute
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        cur = conn.cursor()
        
        cur.execute(sql)
        
        cur.close()
        conn.close()
        
        print("✅ Wallet tables successfully created/updated in PostgreSQL.")
    except Exception as e:
        print(f"❌ Error during database initialization: {e}")

if __name__ == "__main__":
    run_wallet_init()
