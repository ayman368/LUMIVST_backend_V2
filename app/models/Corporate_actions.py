# # backend/app/models/corporate_actions.py
# """
# DB model for tracking Saudi Exchange corporate actions detected by
# corporate_actions_watcher.py.

# Unique key = (symbol, issue_type, eligibility_date) — a symbol can appear
# multiple times with different actions/dates (e.g. MAADEN, BAHRI, TAPRCO all
# had several separate capital events over the years).
# """
# from sqlalchemy import Column, Integer, String, Date, DateTime, Boolean, BigInteger, UniqueConstraint
# from app.core.database import Base  # adjust to match your Base import


# class CorporateAction(Base):
#     __tablename__ = "corporate_actions"

#     id = Column(Integer, primary_key=True, autoincrement=True)
#     symbol = Column(String(10), nullable=False, index=True)
#     company_name = Column(String(255))
#     recommendation_announcement_date = Column(Date, nullable=True)
#     issue_type = Column(String(100), nullable=False)
#     eligibility_date = Column(Date, nullable=False)
#     new_capital = Column(BigInteger, nullable=True)
#     previous_capital = Column(BigInteger, nullable=True)
#     classification = Column(String(20), nullable=False)  # AUTO_ADJUST / NEEDS_REVIEW
#     processed = Column(Boolean, default=False)
#     detected_at = Column(DateTime, nullable=False)

#     __table_args__ = (
#         UniqueConstraint("symbol", "issue_type", "eligibility_date", name="uq_corporate_action"),
#     )