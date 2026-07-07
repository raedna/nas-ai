from pathlib import Path

from core.db import get_conn


SCHEMA_PATH = Path(__file__).with_name("fix_schema.sql")


def init_fix_analysis_storage() -> None:
    sql = SCHEMA_PATH.read_text(encoding="utf-8")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


if __name__ == "__main__":
    init_fix_analysis_storage()
    print("FIX analysis storage schema initialized.")