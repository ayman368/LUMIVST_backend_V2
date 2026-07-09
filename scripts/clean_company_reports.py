import sys
import os

# Add parent directory to path to allow importing app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import argparse

# Load env variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

def clean_company_reports(symbol, excel_only=False, force=False, lang=None):
    target_str = "EXCEL reports" if excel_only else "ALL reports"
    if lang:
        target_str += f" in language '{lang}'"
    print(f"⚠️  WARNING: You are about to DELETE {target_str} for symbol '{symbol}'.")
    print("This includes Database records AND Cloudflare R2 files.")
    
    if not force:
        confirm = input("Are you sure? (type 'yes' to confirm): ")
        if confirm.lower() != 'yes':
            print("Operation cancelled.")
            return

    # 1. Cleaner DB
    if DATABASE_URL:
        try:
            from app.core.database import SessionLocal
            from app.models.official_filings import CompanyOfficialFiling, FileType
            
            db = SessionLocal()
            try:
                query = db.query(CompanyOfficialFiling).filter(CompanyOfficialFiling.company_symbol == symbol)
                
                if excel_only:
                    # Using the Enum object directly is safer with SQLAlchemy
                    query = query.filter(CompanyOfficialFiling.file_type == FileType.EXCEL)
                if lang:
                    query = query.filter(CompanyOfficialFiling.language == lang)
                
                deleted_count = query.delete(synchronize_session=False)
                db.commit()
                print(f"✅ Deleted {deleted_count} records from Database.")
            except Exception as e:
                db.rollback()
                print(f"❌ Database Query Error: {e}")
            finally:
                db.close()

        except Exception as e:
            print(f"❌ Database Connection Error: {e}")
    else:
        print("❌ DATABASE_URL not found.")

    # 2. Clean R2
    if S3_ENDPOINT and S3_ACCESS_KEY and S3_SECRET_KEY:
        try:
            s3 = boto3.resource('s3',
                endpoint_url=S3_ENDPOINT,
                aws_access_key_id=S3_ACCESS_KEY,
                aws_secret_access_key=S3_SECRET_KEY
            )
            bucket = s3.Bucket(S3_BUCKET_NAME)
            
            print(f"⏳ Deleting files from R2 bucket '{S3_BUCKET_NAME}' with prefix '{symbol}/'...")
            
            deleted_count = 0
            for obj in bucket.objects.filter(Prefix=f"{symbol}/"):
                key = obj.key.lower()
                
                # Check Language (Format: symbol/year/lang/filename)
                if lang and f"/{lang.lower()}/" not in key:
                    continue
                    
                if excel_only:
                    if not (key.endswith('.xls') or key.endswith('.xlsx')):
                        continue
                        
                obj.delete()
                deleted_count += 1
                
            print(f"✅ Deleted {deleted_count} files from R2.")
            
        except Exception as e:
            print(f"❌ S3/R2 Error: {e}")
    else:
        print("❌ S3 Credentials not found.")

    print("\nCleanup Complete. You can now re-ingest.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Clean company reports from DB and R2')
    parser.add_argument('symbol', type=str, help='Company Symbol')
    parser.add_argument('--excel-only', action='store_true', help='Delete only Excel files and records')
    parser.add_argument('--lang', type=str, choices=['en', 'ar'], help='Delete only for a specific language')
    parser.add_argument('--force', action='store_true', help='Skip confirmation prompt')
    
    args = parser.parse_args()
    
    clean_company_reports(args.symbol, args.excel_only, args.force, args.lang)