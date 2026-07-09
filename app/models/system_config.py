from sqlalchemy import Column, String, DateTime
from sqlalchemy.sql import func
from app.core.database import Base


class SystemConfig(Base):
    __tablename__ = "system_config"

    key = Column(String(100), primary_key=True)
    value = Column(String(500), nullable=False)
    data_type = Column(String(20), nullable=True)    # 'float', 'int', 'string', 'date'
    description = Column(String(500), nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def get_typed_value(self):
        """Return value cast to the appropriate Python type."""
        if self.data_type == "float":
            return float(self.value)
        if self.data_type == "int":
            return int(self.value)
        return self.value

    def __repr__(self):
        return f"<SystemConfig key={self.key} value={self.value}>"
