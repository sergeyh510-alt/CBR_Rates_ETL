import logging
from datetime import date, datetime
from decimal import Decimal

import requests
from lxml import etree
from psycopg2.extras import execute_values
import psycopg2

URL = "https://www.cbr.ru/scripts/XML_daily.asp"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "coinlore",
    "user": "postgres",
    "password": "12345",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("cbr")


def fetch_xml(url: str = URL) -> bytes:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.content


def parse_rates(xml_bytes: bytes):
    parser = etree.XMLParser(recover=True, resolve_entities=False, no_network=True)
    root = etree.fromstring(xml_bytes, parser=parser)

    date_attr = root.get("Date")
    rate_date = datetime.strptime(date_attr, "%d.%m.%Y").date()

    def text(el, tag):
        node = el.find(tag)
        return node.text.strip() if node is not None and node.text else None

    def to_decimal(s):
        return Decimal(s.replace(",", ".").replace("\xa0", "").strip()) if s else None

    rates = []
    for v in root.findall("Valute"):
        rates.append({
            "valute_id":  v.get("ID"),
            "num_code":   text(v, "NumCode"),
            "char_code":  text(v, "CharCode"),
            "nominal":    int(text(v, "Nominal") or 1),
            "name":       text(v, "Name"),
            "value":      to_decimal(text(v, "Value")),
            "vunit_rate": to_decimal(text(v, "VunitRate")),
        })
    return rate_date, rates


def save_rates(conn, rate_date: date, rates: list[dict]):
    record_date = date.today()
    rows = [
        (
            record_date, rate_date,
            r["valute_id"], r["num_code"], r["char_code"],
            r["nominal"], r["name"], r["value"], r["vunit_rate"],
        )
        for r in rates
    ]

    INSERT_SQL = """
        INSERT INTO raw.cbr_rates
            (record_date, rate_date, valute_id, num_code, char_code,
             nominal, name, value, vunit_rate)
        VALUES %s
        ON CONFLICT (record_date, rate_date, valute_id) DO UPDATE SET
            num_code   = EXCLUDED.num_code,
            char_code  = EXCLUDED.char_code,
            nominal    = EXCLUDED.nominal,
            name       = EXCLUDED.name,
            value      = EXCLUDED.value,
            vunit_rate = EXCLUDED.vunit_rate
    """

    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, rows, page_size=500)
    conn.commit()
    log.info("Сохранено %d записей (record_date=%s, rate_date=%s)",
             len(rows), record_date, rate_date)


def refresh_dashboard(conn, daily_days: int = 90):
    with conn.cursor() as cur:
        cur.execute("CALL etl.refresh_dashboard(%s, %s, %s)",
                    (None, daily_days, None))
    conn.commit()
    log.info("Витрины dwh.dm_* пересобраны (daily_days=%s)", daily_days)


def main():
    xml_bytes = fetch_xml()
    rate_date, rates = parse_rates(xml_bytes)
    log.info("Получено %d валют, дата курса %s", len(rates), rate_date)

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        save_rates(conn, rate_date, rates)
        refresh_dashboard(conn, daily_days=90)
    finally:
        conn.close()


if __name__ == "__main__":
    main()