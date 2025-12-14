from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Date
from datetime import date
from .database import Base


class RespondentStats(Base):
    __tablename__ = "respondent_stats"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    date: Mapped[date] = mapped_column(Date, index=True)
    respondent: Mapped[int] = mapped_column(index=True)
    sex: Mapped[int] = mapped_column(index=True)
    age: Mapped[int] = mapped_column(index=True)
    weight: Mapped[float] = mapped_column()
