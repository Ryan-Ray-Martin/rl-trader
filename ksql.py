"""Configures KSQL to create new stream of data containing previous prices"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

# KSQLDB cli for crypto

"""{
        'ev': 'XA',
        'pair': 'BTC-USD',
        'v': 21.67807129,
        'vw': 58699.308,
        'z': 0,
        'o': 58662,
        'c': 58761.79,
        'h': 58790.48,
        'l': 58640.7767046,
        's': 1616022720000,
        'e': 1616022780000
    }"""
"""
CREATE STREAM crypto_min_bars_stream (
    ev VARCHAR,
    pair VARCHAR,
    v DOUBLE, 
    o DOUBLE, 
    c DOUBLE, 
    h DOUBLE, 
    l DOUBLE,
    s INT
) WITH (
    kafka_topic = 'crypto_min_bars',
    value_format = 'json'
);
"""
# to view the stream enter:

"""SELECT * FROM crypto_min_bars_stream EMIT CHANGES;"""

# create a table that also creates a topic to stream on faust

"""
CREATE TABLE crypto_window_table AS
SELECT pair, LATEST_BY_OFFSET(v,30) as V,
LATEST_BY_OFFSET(o,30) as O,
LATEST_BY_OFFSET(c,30) as C,
LATEST_BY_OFFSET(h,30) as H,
LATEST_BY_OFFSET(l,30) as L 
FROM crypto_min_bars_stream 
GROUP BY pair
EMIT CHANGES;
"""

# KSQL cli for stocks
 """
CREATE STREAM stock_min_bars_stream (
    ev VARCHAR,
    sym VARCHAR,
    v DOUBLE, 
    o DOUBLE, 
    c DOUBLE, 
    h DOUBLE, 
    l DOUBLE,
    price DOUBLE
) WITH (
    kafka_topic = 'stock_min_bars',
    value_format = 'json'
);
"""
"""
CREATE TABLE stock_sliding_window AS
SELECT sym AS ROWKEY, 
AS_VALUE(sym) as SYMBOL,
LATEST_BY_OFFSET(price,32) as PRICE,
LATEST_BY_OFFSET(v,32) as V,
LATEST_BY_OFFSET(o,32) as O,
LATEST_BY_OFFSET(c,32) as C,
LATEST_BY_OFFSET(h,32) as H,
LATEST_BY_OFFSET(l,32) as L 
FROM stock_min_bars_stream 
GROUP BY sym
EMIT CHANGES;
"""

"""
CREATE TABLE crypto_sliding_window AS
SELECT pair AS ROWKEY, 
AS_VALUE(pair) as SYMBOL,
LATEST_BY_OFFSET(v,32) as V,
LATEST_BY_OFFSET(o,32) as O,
LATEST_BY_OFFSET(c,32) as C,
LATEST_BY_OFFSET(h,32) as H,
LATEST_BY_OFFSET(l,32) as L 
FROM crypto_min_bars_stream 
GROUP BY pair
EMIT CHANGES;
"""


#
# TODO: Complete the following KSQL statements.

# TODO: For the first statement, create a `turnstile` table from your turnstile
#       topic. Make sure to use 'avro' datatype!
"""message 
{
    'ev': 'XQ',
    'pair': 'ETH-USD',
    'lp': 0,
    'ls': 0,
    'bp': 1437.77,
    'bs': 33.9,
    'ap': 1437.78,
    'as': 0.27357865,
    't': 1614568096738,
    'x': 1,
    'r': 1614568096744
}"""

# TODO: For the second statment, create a `turnstile_summary` table by selecting
#       from the `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`
#       Make sure to set the value format to JSON

# REMINDER: ALL INNER STRINGS IN SINGLE QUTOES ONLY!

"""message {'ev': 'A', 'sym': 'NIO', 'v': 629, 'av': 173317619, 'op': 39.24, 'vw': 34.6619, 'o': 34.6601, 'c': 34.6601, 'h': 34.6601, 'l': 34.6601, 'a': 35.4221, 'z': 62, 's': 1614964124000, 'e': 1614964125000}"""
KSQL_STATEMENT = """
CREATE STREAM crypto_prices_stream (
    ev VARCHAR,
    pair VARCHAR,
    lp INT, 
    ls INT, 
    bp DOUBLE, 
    bs DOUBLE, 
    ap DOUBLE, 
    `as` DOUBLE,
    t INT,
    l INT, 
    x INT,
    r INT
) WITH (
    kafka_topic = 'crypto_prices_min',
    value_format = 'json'
);
"""
"""
CREATE STREAM stocks_stream ( ev VARCHAR,
>    sym VARCHAR,
>    v INT, 
>    av INT, 
>    op DOUBLE, 
>    vw DOUBLE, 
>    o DOUBLE, 
>    c DOUBLE,
>    h DOUBLE,
>    l DOUBLE, 
>    a DOUBLE,
>    z INT, 
>    s INT, 
>    e INT) WITH (
>    KAFKA_TOPIC = 'stocks_prices',
>    VALUE_FORMAT = 'JSON'
>  );
"""
"""
CREATE TABLE trade_aggs AS
    SELECT
    LATEST_BY_OFFSET(bs, 10) AS PRICES
FROM stock_prices_stream;
"""
"""CREATE STREAM crypto_sliding_window AS 
SELECT pair, v, o, c, h, l, COUNT(*) AS TOTAL
FROM CRYPTO_MIN_BARS_STREAM WINDOW HOPPING (SIZE 10 MINUTES, ADVANCE BY 1 MINUTES)
GROUP BY pair, v, o, c, h, l;



"""
SELECT ROWKEY, ev, sym, v, av, op, vw, o, c, h, l, a, z, s, e
FROM stock_prices_stream
EMIT;
"""
"""
SELECT sym, LATEST_BY_OFFSET(o) FROM stock_prices_stream GROUP BY sym;

def execute_statement():
    """Executes the KSQL statement against the KSQL API"""


    logging.debug("executing ksql statement...")


    resp = requests.post(
        f"{KSQL_URL}/query",
        headers={"Content-Type": "application/vnd.ksql.v1+json; charset=utf-8"},
        data=json.dumps(
            {
                "ksql": "SELECT * FROM STOCK_PRICES_STREAM;",
                "streamsProperties": {},
            },
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()

def print_statement():
    """Executes the KSQL statement against the KSQL API"""


    logging.debug("executing ksql statement...")


    resp = requests.post(
        f"{KSQL_URL}/query",
        headers={"Content-Type": "application/vnd.ksql.v1+json; charset=utf-8"},
        data=json.dumps(
            {
                "ksql": "SELECT * FROM STOCK_PRICES_STREAM;",
                "streamsProperties": {},
            },
        ),
    )

    print(resp.text)

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()



if __name__ == "__main__":
    print_statement()

# this will work just be sure to connect to correct topic in faust

"""
CREATE TABLE crypto_window_table AS
SELECT pair, LATEST_BY_OFFSET(v,30) as V,
LATEST_BY_OFFSET(o,30) as O,
LATEST_BY_OFFSET(c,30) as C,
LATEST_BY_OFFSET(h,30) as H,
LATEST_BY_OFFSET(l,30) as L 
FROM crypto_min_bars_stream 
GROUP BY pair
EMIT CHANGES;
"""