from app.core.database import SessionLocal
from app.models.system_config import SystemConfig
from datetime import datetime

def seed_missing_configs():
    db = SessionLocal()
    try:
        configs = [
            {"key": "fed_rate_current", "value": "3.75", "data_type": "float", "description": "Current Fed Rate percentage"},
            {"key": "fed_rate_expected", "value": "3.50", "data_type": "float", "description": "Expected Fed Rate percentage"},
            {"key": "tasi_index_level", "value": "11900.0", "data_type": "float", "description": "TASI Index Level"}
        ]
        
        for c in configs:
            existing = db.query(SystemConfig).filter(SystemConfig.key == c["key"]).first()
            if not existing:
                new_config = SystemConfig(
                    key=c["key"],
                    value=c["value"],
                    data_type=c["data_type"],
                    description=c["description"],
                    updated_at=datetime.utcnow()
                )
                db.add(new_config)
                print(f"Inserted missing config: {c['key']}")
            else:
                print(f"Config {c['key']} already exists.")
        
        db.commit()
    except Exception as e:
        db.rollback()
        print("Error:", e)
    finally:
        db.close()

if __name__ == "__main__":
    seed_missing_configs()
