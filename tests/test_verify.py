from natgas_bot.sessions import classify
from natgas_bot.verify import build_report, check_chaining, export_windows_csv, find_gaps, find_ties, session_stats


def test_real_data_chains(loaded_db):
    rows = loaded_db.windows()
    res = check_chaining(rows)
    assert res.pairs_checked == 35
    assert res.mismatches == []
    assert find_ties(rows) == []


def test_gaps_skip_weekend(loaded_db):
    gaps = find_gaps(loaded_db.windows())
    # 01:00-03:45Z Sat (not pulled), Sun->Mon and Mon->Tue fixture holes; the Sat->Sun break is excluded
    assert len(gaps) == 3


def test_detects_mismatch_and_tie(loaded_db):
    # 2nd window gets a target that doesn't match the 1st window's settle
    loaded_db.conn.execute("UPDATE windows SET target = 3.03000 WHERE ticker LIKE '%26SEP181645%'")
    # 4th window forced to a tie, which also breaks its link to the 5th
    loaded_db.conn.execute("UPDATE windows SET settle = target WHERE ticker LIKE '%26SEP181715%'")
    rows = loaded_db.windows()
    assert len(check_chaining(rows).mismatches) == 2
    assert len(find_ties(rows)) == 1


def test_session_stats(loaded_db):
    st = session_stats(loaded_db.windows())
    assert st["sunday_reopen"]["n"] == 1
    assert round(st["sunday_reopen"]["abs_move_c_max"], 3) == 2.784
    assert st["cme_break"]["n"] == 4
    assert st["friday_evening"]["n"] == 17
    assert st["friday_evening"]["abs_move_c_median"] < 0.3


def test_report_and_proxy(loaded_db, tmp_path):
    for r in loaded_db.windows():
        loaded_db.insert_proxy("hyperliquid", "xyz:NATGAS", r["close_ts"] - 2000, {"oraclePx": str(r["settle"] + 0.001)})
    report = build_report(loaded_db)
    assert "35/35 contiguous pairs match" in report
    assert "oracle:xyz:NATGAS" in report and "mean=+0.10" in report
    out = tmp_path / "w.csv"
    assert export_windows_csv(loaded_db, out) == 40
    assert "sunday_reopen" in out.read_text()
