import sqlalchemy


def get_engine():
    return sqlalchemy.create_engine(
        "postgresql://username:password@localhost:5432/fishtank"
    )
