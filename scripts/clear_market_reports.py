import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.orm import Session
from app.core.database import SessionLocal
from app.models.market_reports import (
    SubstantialShareholder,
    NetShortPosition,
    ForeignHeadroom,
    ShareBuyback,
    SBLPosition,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clear_all_market_reports():
    db = SessionLocal()
    try:
        logger.info("🗑️ Starting to clear all market reports data...")
        
        # Delete data from all 5 models
        deleted_sh = db.query(SubstantialShareholder).delete()
        deleted_nsp = db.query(NetShortPosition).delete()
        deleted_fh = db.query(ForeignHeadroom).delete()
        deleted_sb = db.query(ShareBuyback).delete()
        deleted_sbl = db.query(SBLPosition).delete()
        
        # Commit the transaction
        db.commit()
        
        logger.info(f"✅ Cleared {deleted_sh} Substantial Shareholders records.")
        logger.info(f"✅ Cleared {deleted_nsp} Net Short Positions records.")
        logger.info(f"✅ Cleared {deleted_fh} Foreign Headroom records.")
        logger.info(f"✅ Cleared {deleted_sb} Share Buybacks records.")
        logger.info(f"✅ Cleared {deleted_sbl} SBL Positions records.")
        
        logger.info("🎉 All market reports data has been completely erased. You can now start fresh!")
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Error clearing data: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    clear_all_market_reports()
