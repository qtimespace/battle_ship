"""Unit tests for bot.py logic (без aiogram/DB)."""
import os
import sys
import types
import json
import importlib.util

import pytest


def _import_bot():
    """Import bot.py with stubbed aiogram + dummy env so no I/O happens."""
    fake = types.ModuleType("aiogram")
    fake.Bot = lambda **k: None
    fake.Dispatcher = lambda *a, **k: type("D", (), {
        "message_handler": lambda *a, **k: (lambda f: f)
    })()
    fake.types = types.ModuleType("aiogram.types")
    fake.types.Message = object
    fake_utils = types.ModuleType("aiogram.utils")
    fake_utils.executor = types.ModuleType("aiogram.utils.executor")
    fake_utils.executor.start_polling = lambda *a, **k: None
    sys.modules.setdefault("aiogram", fake)
    sys.modules.setdefault("aiogram.types", fake.types)
    sys.modules.setdefault("aiogram.utils", fake_utils)
    sys.modules.setdefault("aiogram.utils.executor", fake_utils.executor)

    os.environ.setdefault("BOT_TOKEN", "test")
    os.environ.setdefault("DATABASE_URL", "postgresql://stub/stub")

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    spec = importlib.util.spec_from_file_location("bot", os.path.join(root, "bot.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.fixture(scope="module")
def bot():
    return _import_bot()


# ---------- parse_move ----------

@pytest.mark.parametrize("text,expected", [
    ("A1", (0, 0)),
    ("a1", (0, 0)),
    (" B7 ", (1, 6)),
    ("J10", (9, 9)),
    ("j 10", (9, 9)),
])
def test_parse_move_valid(bot, text, expected):
    assert bot.parse_move(text) == expected


@pytest.mark.parametrize("text", ["", "Z1", "A0", "A11", "AA", "123", "A1B"])
def test_parse_move_invalid(bot, text):
    assert bot.parse_move(text) is None


# ---------- neighbors ----------

def test_neighbors_corner(bot):
    n = set(bot.neighbors((0, 0)))
    assert n == {(0, 1), (1, 0), (1, 1)}


def test_neighbors_center_count(bot):
    n = list(bot.neighbors((5, 5)))
    assert len(n) == 8


# ---------- place_fleet ----------

def test_place_fleet_counts(bot):
    ships = bot.place_fleet()
    sizes = sorted((len(s) for s in ships), reverse=True)
    assert sizes == sorted(bot.FLEET, reverse=True)
    flat = [c for s in ships for c in s]
    assert len(flat) == sum(bot.FLEET)
    assert len(set(flat)) == len(flat)  # no duplicate cells


def test_place_fleet_no_touch(bot):
    ships = bot.place_fleet()
    for i, a in enumerate(ships):
        for b in ships[i + 1:]:
            for c in a:
                neigh = set(bot.neighbors(c))
                assert not (neigh & b), f"ships touch via {c}"
                assert c not in b


def test_place_fleet_in_bounds(bot):
    ships = bot.place_fleet()
    for s in ships:
        for (x, y) in s:
            assert 0 <= x < bot.FIELD
            assert 0 <= y < bot.FIELD


# ---------- ship shape (lines only) ----------

def test_ships_are_lines(bot):
    ships = bot.place_fleet()
    for s in ships:
        xs = {x for x, _ in s}
        ys = {y for _, y in s}
        assert len(xs) == 1 or len(ys) == 1, "ship must be horizontal or vertical"


# ---------- new_player / reroll ----------

def test_new_player_shape(bot):
    p = bot.new_player()
    assert p["ready"] is False
    for k in ("ships", "ships_cells", "incoming_hits", "incoming_misses",
              "shots_hit", "shots_miss"):
        assert k in p


def test_reroll_changes_ships(bot):
    p = bot.new_player()
    bot.reroll(p)
    assert len(p["ships"]) == len(bot.FLEET)
    assert len(p["ships_cells"]) == len(bot.FLEET)


# ---------- serialization round-trip ----------

def test_serialize_roundtrip(bot):
    p1 = bot.new_player(); bot.reroll(p1)
    p2 = bot.new_player(); bot.reroll(p2)
    p1["shots_miss"].add((0, 0))
    p1["shots_hit"].add((1, 1))
    p2["incoming_misses"].add((0, 0))
    p2["incoming_hits"].add((1, 1))
    p1["ready"] = True
    game = {
        "code": "TEST01", "state": "PLAYING", "turn": 42, "host": 42,
        "players": {42: p1, 43: p2},
    }
    raw = bot.serialize_game(game)
    # simulate DB round-trip via JSON
    row = {"code": "TEST01", "state": "PLAYING", "turn": 42, "host": 42,
           "data": json.dumps(raw)}
    restored = bot.deserialize_game(row)
    assert restored["state"] == "PLAYING"
    assert restored["turn"] == 42
    assert set(restored["players"].keys()) == {42, 43}
    assert restored["players"][42]["ready"] is True
    assert (0, 0) in restored["players"][42]["shots_miss"]
    assert (1, 1) in restored["players"][42]["shots_hit"]
    # cells are tuples again
    cell = next(iter(restored["players"][42]["ships"][0]["orig"]))
    assert isinstance(cell, tuple)


# ---------- join rate limit ----------

def test_join_rate_limit(bot):
    bot._join_attempts.clear()
    uid = 99_999
    for _ in range(bot.JOIN_MAX_ATTEMPTS):
        assert bot.join_allowed(uid)
    assert not bot.join_allowed(uid)


# ---------- other() invariant ----------

def test_other_requires_two_players(bot):
    g = {"players": {1: {}, 2: {}}}
    assert bot.other(g, 1) == 2
    assert bot.other(g, 2) == 1
    with pytest.raises(RuntimeError):
        bot.other({"players": {1: {}}}, 1)
