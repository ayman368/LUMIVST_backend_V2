import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("❌ ERROR: DATABASE_URL not found in .env")
    sys.exit(1)

def migrate_db():
    print("Starting Wallet Database Migration (Adding user_id)...")
    engine = create_engine(DATABASE_URL)
    
    tables = [
        "wallet_positions",
        "wallet_trades",
        "wallet_settings",
        "wallet_weekly_studies"
    ]
    
    with engine.connect() as conn:
        for table in tables:
            print(f"Processing table: {table}")
            
            # Check if column already exists
            result = conn.execute(text(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='{table}' AND column_name='user_id';
            """))
            if result.fetchone():
                print(f"Column user_id already exists in {table}, skipping.")
                continue
                
            try:
                # 1. Add column allowing NULL first
                print(f"   -> Adding user_id column...")
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN user_id INTEGER;"))
                
                # 2. Set default value to 1 for existing rows
                # Assumes user ID 1 exists (usually the admin)
                print(f"   -> Setting default user_id to 1 for existing records...")
                conn.execute(text(f"UPDATE {table} SET user_id = 1;"))
                
                # 3. Alter column to NOT NULL
                print(f"   -> Enforcing NOT NULL constraint...")
                conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN user_id SET NOT NULL;"))
                
                # 4. Add Foreign Key constraint
                print(f"   -> Adding Foreign Key constraint to users(id)...")
                conn.execute(text(f"""
                    ALTER TABLE {table} 
                    ADD CONSTRAINT fk_{table}_user_id 
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;
                """))
                
                # 5. Add Index for performance
                print(f"   -> Adding Index on user_id...")
                conn.execute(text(f"CREATE INDEX idx_{table}_user_id ON {table}(user_id);"))
                
                conn.commit()
                print(f"Successfully migrated {table}!\n")
                
            except Exception as e:
                conn.rollback()
                print(f"Error migrating {table}: {e}")
                
    print("All Wallet tables have been successfully migrated for Multi-User Support!")

if __name__ == "__main__":
    migrate_db()
