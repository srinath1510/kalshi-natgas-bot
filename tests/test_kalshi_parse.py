from natgas_bot.kalshi import iso_to_ms, parse_market, parse_orderbook, parse_trade


def test_parse_real_settled_market(real_market_raw):
    w = parse_market(real_market_raw)
    assert w.ticker == "KXNATGAS15M-26SEP221500-00"
    assert w.target == 3.13377 and w.settle == 3.13701
    assert w.result == "yes" and w.status == "finalized"
    assert w.volume == 12362.35 and w.open_interest == 4654.99
    assert w.close_ts - w.open_ts == 15 * 60 * 1000
    assert w.settlement_ts - w.close_ts == 18_256


def test_unsettled_market_has_no_settle_or_result():
    w = parse_market({"ticker": "X", "open_time": "2026-09-22T18:45:00Z", "floor_strike": 3.1,
                      "expiration_value": "", "result": ""})
    assert w.settle is None and w.result is None and w.close_ts is None


def test_iso_fraction_lengths():
    base = iso_to_ms("2026-09-22T19:00:18Z")
    assert iso_to_ms("2026-09-22T19:00:18.2Z") == base + 200
    assert iso_to_ms("2026-09-22T19:00:18.21049Z") == base + 210
    assert iso_to_ms("2026-09-22T19:00:18.123456789Z") == base + 123
    assert iso_to_ms(None) is None


def test_orderbook_legacy_cents():
    book = parse_orderbook({"orderbook": {"yes": [[45, 100], [47, 20], [30, 0]], "no": [[50, 10], [52, 30]]}})
    assert book.best_yes_bid == (0.47, 20)
    assert book.best_yes_ask == (0.48, 30)
    assert (0.3, 0) not in book.yes  # empty levels dropped


def test_orderbook_fixed_point_dollars():
    book = parse_orderbook({"orderbook_fp": {"yes_dollars": [["0.4500", "100.00"]], "no_dollars": [["0.5200", "30.00"]]}})
    assert book.best_yes_bid == (0.45, 100.0)
    assert book.best_yes_ask == (0.48, 30.0)


def test_orderbook_empty():
    book = parse_orderbook({"orderbook": {"yes": None, "no": None}})
    assert book.best_yes_bid is None and book.best_yes_ask is None


def test_trade_both_formats():
    new = parse_trade({"trade_id": "a", "ticker": "T", "created_time": "2026-09-22T18:50:01.5Z",
                       "yes_price_dollars": "0.6100", "no_price_dollars": "0.3900", "count_fp": "12.00", "taker_side": "yes"})
    old = parse_trade({"trade_id": "b", "ticker": "T", "created_time": "2026-09-22T18:50:02Z",
                       "yes_price": 61, "no_price": 39, "count": 12, "taker_side": "no"})
    for t in (new, old):
        assert t.yes_price == 0.61 and t.no_price == 0.39 and t.count == 12.0
