from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Project-wide declarative base. All ORM models inherit from this."""

    def to_dict(self, exclude_columns: frozenset[str] = frozenset()) -> dict:
        return {
            c.name: getattr(self, c.name)
            for c in self.__table__.columns
            if c.name not in exclude_columns
        }
