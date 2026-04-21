import asyncio
import json
import logging
import os
import random
import re
import string
from urllib.parse import quote
import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("battleship")

API_TOKEN = os.getenv("BOT_TOKEN")
if not API_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN env var is not set. Получи токен у @BotFather и задай переменную окружения."
    )

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL env var is not set.")

db_pool: asyncpg.Pool = None  # type: ignore

bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

FIELD = 10
LETTERS = "ABCDEFGHIJ"
FLEET = [4, 3, 3, 2, 2, 2, 1, 1, 1, 1]
CODE_RE = re.compile(r"^[A-Z0-9]{6}$")

# username бота, узнаётся в on_startup; нужен для deep-link приглашений
BOT_USERNAME = None


def kb_menu(state=None):
    """Контекстная reply-клавиатура с командами."""
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    if state == "PLACING":
        kb.row(types.KeyboardButton("/replace"), types.KeyboardButton("/ready"))
        kb.row(types.KeyboardButton("/surrender"))
    elif state == "PLAYING":
        kb.row(types.KeyboardButton("/surrender"))
        kb.row(types.KeyboardButton("/help"))
    else:
        kb.row(types.KeyboardButton("/new"))
        kb.row(types.KeyboardButton("/help"))
    return kb


def kb_invite(code):
    """Inline-кнопка с share-sheet Telegram и deep-link'ом в игру."""
    deep = f"https://t.me/{BOT_USERNAME}?start={code}"
    text = f"Зову тебя в морской бой! Жми, чтобы войти: {deep}"
    share = (
        "https://t.me/share/url?"
        f"url={quote(deep, safe='')}&text={quote(text, safe='')}"
    )
    m = types.InlineKeyboardMarkup(row_width=1)
    m.add(types.InlineKeyboardButton("📨 Позвать друга", url=share))
    return m


def _user_state(uid):
    code = user_game.get(uid)
    if not code or code not in games:
        return None
    return games[code]["state"]

JOIN_WINDOW_SEC = 60
JOIN_MAX_ATTEMPTS = 5
# user_id -> list of recent attempt timestamps
_join_attempts: dict = {}

TURN_TIMEOUT_SEC = 600  # 10 минут на ход; иначе автопоражение
# code -> asyncio.Task
_turn_timers: dict = {}

# Пауза перед стартом polling, чтобы Telegram освободил предыдущий
# getUpdates после редеплоя на Railway (иначе TerminatedByOtherGetUpdates).
STARTUP_DELAY_SEC = int(os.getenv("STARTUP_DELAY_SEC", "15"))


def join_allowed(user_id):
    import time
    now = time.monotonic()
    buf = _join_attempts.setdefault(user_id, [])
    buf[:] = [t for t in buf if now - t < JOIN_WINDOW_SEC]
    if len(buf) >= JOIN_MAX_ATTEMPTS:
        return False
    buf.append(now)
    return True

# code -> game dict
games = {}
# user_id -> code
user_game = {}


def new_code():
    while True:
        code = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
        if code not in games:
            return code


# ---------- Persistence ----------

def _cells_to_list(cells):
    return [list(c) for c in cells]


def _cells_to_set(raw):
    return {tuple(c) for c in raw}


def serialize_game(game):
    players = {}
    for uid, p in game["players"].items():
        players[str(uid)] = {
            "ready": p["ready"],
            "ships": [
                {"orig": _cells_to_list(s["orig"]), "alive": _cells_to_list(s["alive"])}
                for s in p["ships"]
            ],
            "incoming_hits": _cells_to_list(p["incoming_hits"]),
            "incoming_misses": _cells_to_list(p["incoming_misses"]),
            "shots_hit": _cells_to_list(p["shots_hit"]),
            "shots_miss": _cells_to_list(p["shots_miss"]),
        }
    return {"players": players}


def deserialize_game(row):
    data = row["data"]
    if isinstance(data, str):
        data = json.loads(data)
    players = {}
    for uid_s, p in data["players"].items():
        ships = [
            {"orig": _cells_to_set(s["orig"]), "alive": _cells_to_set(s["alive"])}
            for s in p["ships"]
        ]
        players[int(uid_s)] = {
            "ready": p["ready"],
            "ships": ships,
            "ships_cells": [s["orig"] for s in ships],
            "incoming_hits": _cells_to_set(p["incoming_hits"]),
            "incoming_misses": _cells_to_set(p["incoming_misses"]),
            "shots_hit": _cells_to_set(p["shots_hit"]),
            "shots_miss": _cells_to_set(p["shots_miss"]),
        }
    return {
        "code": row["code"],
        "state": row["state"],
        "turn": row["turn"],
        "host": row["host"],
        "players": players,
    }


async def save_game(code):
    game = games.get(code)
    if not game:
        return
    payload = json.dumps(serialize_game(game))
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO games (code, state, turn, host, data, updated_at)
                VALUES ($1, $2, $3, $4, $5::jsonb, NOW())
                ON CONFLICT (code) DO UPDATE SET
                    state=EXCLUDED.state,
                    turn=EXCLUDED.turn,
                    data=EXCLUDED.data,
                    updated_at=NOW()
                """,
                code, game["state"], game["turn"], game["host"], payload,
            )
            await conn.execute("DELETE FROM user_game WHERE code=$1", code)
            if game["players"]:
                await conn.executemany(
                    "INSERT INTO user_game (user_id, code) VALUES ($1, $2) "
                    "ON CONFLICT (user_id) DO UPDATE SET code=EXCLUDED.code",
                    [(uid, code) for uid in game["players"].keys()],
                )


async def delete_game(code):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM games WHERE code=$1", code)


def cancel_turn_timer(code):
    t = _turn_timers.pop(code, None)
    if t and not t.done():
        t.cancel()


async def _turn_timeout_handler(code, expected_turn):
    try:
        await asyncio.sleep(TURN_TIMEOUT_SEC)
    except asyncio.CancelledError:
        return
    game = games.get(code)
    if not game or game["state"] != "PLAYING" or game["turn"] != expected_turn:
        return
    loser = expected_turn
    try:
        winner = other(game, loser)
    except RuntimeError:
        return
    log.info("turn timeout code=%s loser=%s winner=%s", code, loser, winner)
    for pid in list(game["players"].keys()):
        user_game.pop(pid, None)
    games.pop(code, None)
    _turn_timers.pop(code, None)
    try:
        await delete_game(code)
    except Exception:
        log.exception("delete_game on timeout failed")
    for pid, text in ((loser, "⌛ Время хода вышло. 💀 Поражение."),
                      (winner, "⌛ Соперник не сделал ход вовремя. 🏆 Победа!")):
        try:
            await bot.send_message(pid, text, reply_markup=kb_menu())
        except Exception:
            pass


def schedule_turn_timer(code):
    cancel_turn_timer(code)
    game = games.get(code)
    if not game or game["state"] != "PLAYING" or game["turn"] is None:
        return
    _turn_timers[code] = asyncio.create_task(
        _turn_timeout_handler(code, game["turn"])
    )


async def load_state():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT code, state, turn, host, data FROM games")
    for r in rows:
        g = deserialize_game(r)
        games[g["code"]] = g
        for uid in g["players"].keys():
            user_game[uid] = g["code"]
    log.info("restored %d games, %d user mappings", len(games), len(user_game))
    for code, g in games.items():
        if g["state"] == "PLAYING" and g["turn"]:
            schedule_turn_timer(code)


def neighbors(cell):
    x, y = cell
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            if dx == 0 and dy == 0:
                continue
            nx, ny = x + dx, y + dy
            if 0 <= nx < FIELD and 0 <= ny < FIELD:
                yield (nx, ny)


def place_fleet():
    """Random placement respecting no-touch rule. Returns list of ships (each a set of cells)."""
    for _ in range(500):
        ships = []
        occupied = set()
        buffer = set()
        ok = True
        for size in FLEET:
            placed = False
            for _ in range(300):
                horiz = random.random() < 0.5
                if horiz:
                    x = random.randint(0, FIELD - size)
                    y = random.randint(0, FIELD - 1)
                    cells = {(x + i, y) for i in range(size)}
                else:
                    x = random.randint(0, FIELD - 1)
                    y = random.randint(0, FIELD - size)
                    cells = {(x, y + i) for i in range(size)}
                if cells & occupied or cells & buffer:
                    continue
                ships.append(cells)
                occupied |= cells
                for c in cells:
                    for n in neighbors(c):
                        buffer.add(n)
                placed = True
                break
            if not placed:
                ok = False
                break
        if ok:
            return ships
    raise RuntimeError("Не удалось расставить флот")


def parse_move(text):
    text = text.strip().upper().replace(" ", "")
    if len(text) < 2 or len(text) > 3:
        return None
    letter = text[0]
    if letter not in LETTERS:
        return None
    try:
        num = int(text[1:])
    except ValueError:
        return None
    if not 1 <= num <= FIELD:
        return None
    return (LETTERS.index(letter), num - 1)


def render(player, show_ships):
    """Однобайтный grid: каждая клетка — 1 ASCII-символ, разделитель — пробел.
    Рамки и штриховка убраны: в мобильных моно-шрифтах Telegram они плывут.
    """
    EMPTY, SHIP, HIT, MISS = ".", "#", "X", "o"
    header = "   " + " ".join(LETTERS)
    lines = [header]
    for y in range(FIELD):
        row = []
        for x in range(FIELD):
            cell = (x, y)
            if show_ships:
                if cell in player["incoming_hits"]:
                    row.append(HIT)
                elif cell in player["incoming_misses"]:
                    row.append(MISS)
                elif any(cell in s for s in player["ships_cells"]):
                    row.append(SHIP)
                else:
                    row.append(EMPTY)
            else:
                if cell in player["shots_hit"]:
                    row.append(HIT)
                elif cell in player["shots_miss"]:
                    row.append(MISS)
                else:
                    row.append(EMPTY)
        lines.append(f"{y + 1:>2} " + " ".join(row))
    return "<pre>" + "\n".join(lines) + "</pre>"


def new_player():
    return {
        "ready": False,
        "ships": [],            # list of {"orig": set, "alive": set}
        "ships_cells": [],      # flat union for rendering
        "incoming_hits": set(),
        "incoming_misses": set(),
        "shots_hit": set(),
        "shots_miss": set(),
    }


def reroll(player):
    ships = place_fleet()
    player["ships"] = [{"orig": set(s), "alive": set(s)} for s in ships]
    player["ships_cells"] = [s["orig"] for s in player["ships"]]


async def send_boards(game, user_id, prefix="", reply_markup=None):
    p = game["players"][user_id]
    own = render(p, show_ships=True)
    has_opponent = len(game["players"]) >= 2
    if has_opponent and game["state"] == "PLAYING":
        enemy = render(p, show_ships=False)
        text = (
            f"{prefix}\n"
            f"🎯 Поле противника (твои выстрелы):\n{enemy}\n"
            f"🚢 Твоё поле:\n{own}"
        )
    else:
        text = f"{prefix}\n🚢 Твоё поле:\n{own}"
    await bot.send_message(
        user_id, text, parse_mode="HTML", reply_markup=reply_markup
    )


def other(game, user_id):
    rest = [u for u in game["players"] if u != user_id]
    if len(rest) != 1:
        raise RuntimeError(f"other(): expected 2 players, got {len(game['players'])}")
    return rest[0]


def safe_reroll(player):
    """Reroll with fallback; raises only after multiple attempts."""
    for _ in range(3):
        try:
            reroll(player)
            return
        except RuntimeError:
            log.warning("place_fleet failed, retrying")
    raise RuntimeError("Не удалось сгенерировать расстановку.")


HELP_TEXT = (
    "🚢 <b>Морской бой</b>\n\n"
    "/new — создать игру и позвать друга\n"
    "/join КОД — присоединиться по коду вручную\n"
    "/replace — перекинуть расстановку\n"
    "/ready — готов к бою\n"
    "/surrender — сдаться\n\n"
    "Ход вводится координатой: <code>A1</code>, <code>B7</code>, <code>J10</code>\n"
    "Обозначения: <code>#</code> — твой корабль, <code>X</code> — попадание, "
    "<code>o</code> — промах, <code>.</code> — пусто."
)


async def _send_help(message):
    await message.reply(
        HELP_TEXT,
        parse_mode="HTML",
        reply_markup=kb_menu(_user_state(message.from_user.id)),
    )


@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    arg = (message.get_args() or "").strip().upper()
    if arg and CODE_RE.match(arg):
        await _try_join(message, arg)
        return
    await _send_help(message)


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    await _send_help(message)


@dp.message_handler(commands=["new"])
async def cmd_new(message: types.Message):
    uid = message.from_user.id
    if uid in user_game:
        await message.reply(
            "Ты уже в игре. /surrender чтобы выйти.",
            reply_markup=kb_menu(_user_state(uid)),
        )
        return
    code = new_code()
    game = {
        "code": code,
        "state": "WAITING",
        "players": {},
        "turn": None,
        "host": uid,
    }
    game["players"][uid] = new_player()
    try:
        safe_reroll(game["players"][uid])
    except RuntimeError as e:
        log.exception("new: reroll failed")
        await message.reply(str(e))
        return
    games[code] = game
    user_game[uid] = code
    await save_game(code)
    log.info("game created code=%s host=%s", code, uid)
    await send_boards(
        game, uid, f"🎲 Игра создана. Код: <code>{code}</code>\nТвоя расстановка:",
        reply_markup=kb_menu("PLACING"),
    )
    deep = f"https://t.me/{BOT_USERNAME}?start={code}"
    await bot.send_message(
        uid,
        f"Позови соперника — кнопка ниже откроет список контактов Telegram.\n"
        f"Ссылка для ручной отправки: {deep}",
        parse_mode="HTML",
        reply_markup=kb_invite(code),
        disable_web_page_preview=True,
    )


async def _try_join(message: types.Message, code: str):
    """Общая join-логика: из /join <код> и из deep-link /start <код>."""
    uid = message.from_user.id
    if uid in user_game:
        await message.reply(
            "Ты уже в игре. /surrender чтобы выйти.",
            reply_markup=kb_menu(_user_state(uid)),
        )
        return
    code = code.strip().upper()
    if not CODE_RE.match(code):
        await message.reply(
            "Код должен состоять из 6 символов A–Z или 0–9.",
            reply_markup=kb_menu(),
        )
        return
    if not join_allowed(uid):
        await message.reply(
            f"Слишком много попыток. Подожди минуту (лимит {JOIN_MAX_ATTEMPTS}/мин).",
            reply_markup=kb_menu(),
        )
        log.warning("join rate-limit uid=%s", uid)
        return
    game = games.get(code)
    if not game:
        await message.reply("Игра с таким кодом не найдена.", reply_markup=kb_menu())
        return
    if game["state"] != "WAITING" or len(game["players"]) >= 2:
        await message.reply("Игра уже идёт или завершена.", reply_markup=kb_menu())
        return
    game["state"] = "PLACING"  # занимаем слот до await, чтобы закрыть race-окно
    game["players"][uid] = new_player()
    try:
        safe_reroll(game["players"][uid])
    except RuntimeError as e:
        log.exception("join: reroll failed")
        game["players"].pop(uid, None)
        game["state"] = "WAITING"
        await message.reply(str(e), reply_markup=kb_menu())
        return
    user_game[uid] = code
    await save_game(code)
    log.info("player %s joined code=%s", uid, code)
    await message.reply(
        "✅ Присоединился. /replace — перекинуть расстановку, /ready — готов к бою.",
        reply_markup=kb_menu("PLACING"),
    )
    await send_boards(game, uid, "Твоя расстановка:")
    await bot.send_message(
        game["host"],
        "🎮 Соперник подключился! Жми /ready когда готов.",
        reply_markup=kb_menu("PLACING"),
    )


@dp.message_handler(commands=["join"])
async def cmd_join(message: types.Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            "Формат: /join КОД",
            reply_markup=kb_menu(_user_state(message.from_user.id)),
        )
        return
    await _try_join(message, parts[1])


@dp.message_handler(commands=["replace"])
async def cmd_replace(message: types.Message):
    uid = message.from_user.id
    code = user_game.get(uid)
    if not code:
        await message.reply("Ты не в игре.", reply_markup=kb_menu())
        return
    game = games[code]
    if game["state"] not in ("WAITING", "PLACING"):
        await message.reply(
            "Бой уже начался, расстановку менять нельзя.",
            reply_markup=kb_menu(game["state"]),
        )
        return
    p = game["players"][uid]
    if p["ready"]:
        await message.reply(
            "Ты уже нажал /ready.", reply_markup=kb_menu("PLACING")
        )
        return
    try:
        safe_reroll(p)
    except RuntimeError as e:
        await message.reply(str(e), reply_markup=kb_menu("PLACING"))
        return
    await save_game(code)
    await send_boards(
        game, uid, "Новая расстановка:", reply_markup=kb_menu("PLACING")
    )


@dp.message_handler(commands=["ready"])
async def cmd_ready(message: types.Message):
    uid = message.from_user.id
    code = user_game.get(uid)
    if not code:
        await message.reply("Ты не в игре.", reply_markup=kb_menu())
        return
    game = games[code]
    if game["state"] not in ("WAITING", "PLACING"):
        await message.reply("Бой уже идёт.", reply_markup=kb_menu("PLAYING"))
        return
    if len(game["players"]) < 2:
        await message.reply(
            "Ждём второго игрока. Позови через кнопку «Позвать друга» в сообщении с кодом.",
            reply_markup=kb_menu("PLACING"),
        )
        return
    if game["players"][uid]["ready"]:
        await message.reply(
            "Ты уже готов, ждём соперника.", reply_markup=kb_menu("PLACING")
        )
        return
    game["players"][uid]["ready"] = True
    opp = other(game, uid)
    if game["players"][opp]["ready"]:
        game["state"] = "PLAYING"
        game["turn"] = random.choice(list(game["players"].keys()))
        first = game["turn"]
        second = other(game, first)
        await save_game(code)
        schedule_turn_timer(code)
        await message.reply("✔ Готов.", reply_markup=kb_menu("PLAYING"))
        await bot.send_message(
            first,
            f"🔫 Твой ход. Координата, например B7. На ход — {TURN_TIMEOUT_SEC // 60} мин.",
            reply_markup=kb_menu("PLAYING"),
        )
        await bot.send_message(
            second, "⏳ Ход соперника.", reply_markup=kb_menu("PLAYING")
        )
    else:
        await save_game(code)
        await message.reply("✔ Готов.", reply_markup=kb_menu("PLACING"))
        await bot.send_message(
            opp,
            "Соперник готов. Жми /ready когда расставишь корабли.",
            reply_markup=kb_menu("PLACING"),
        )


@dp.message_handler(commands=["surrender"])
async def cmd_surrender(message: types.Message):
    uid = message.from_user.id
    code = user_game.get(uid)
    if not code:
        await message.reply("Ты не в игре.", reply_markup=kb_menu())
        return
    game = games[code]
    log.info("surrender uid=%s code=%s", uid, code)
    cancel_turn_timer(code)
    await message.reply("🏳 Ты сдался.", reply_markup=kb_menu())
    for pid in list(game["players"].keys()):
        user_game.pop(pid, None)
        if pid != uid:
            try:
                await bot.send_message(
                    pid, "🏆 Соперник сдался. Победа!", reply_markup=kb_menu()
                )
            except Exception:
                pass
    games.pop(code, None)
    await delete_game(code)


@dp.message_handler()
async def handle_move(message: types.Message):
    uid = message.from_user.id
    code = user_game.get(uid)
    if not code:
        return
    game = games[code]
    if game["state"] != "PLAYING":
        return
    if game["turn"] != uid:
        await message.reply("⏳ Сейчас не твой ход.")
        return
    move = parse_move(message.text)
    if not move:
        await message.reply("Формат: A1, B7, J10")
        return
    shooter = game["players"][uid]
    if move in shooter["shots_hit"] or move in shooter["shots_miss"]:
        await message.reply("Ты уже стрелял сюда.")
        return
    if any(move in s["orig"] for s in shooter["ships"]):
        await message.reply("Это твоя клетка, стреляй по полю соперника.")
        return

    opp_id = other(game, uid)
    opp = game["players"][opp_id]

    coord_name = f"{LETTERS[move[0]]}{move[1] + 1}"

    hit_ship = None
    for ship in opp["ships"]:
        if move in ship["alive"]:
            hit_ship = ship
            break

    if hit_ship is None:
        shooter["shots_miss"].add(move)
        opp["incoming_misses"].add(move)
        game["turn"] = opp_id
        await save_game(code)
        schedule_turn_timer(code)
        await send_boards(game, uid, f"🌊 Мимо ({coord_name}). Ход соперника.")
        await send_boards(game, opp_id, f"Соперник стрелял {coord_name} — мимо. Твой ход.")
        return

    hit_ship["alive"].remove(move)
    shooter["shots_hit"].add(move)
    opp["incoming_hits"].add(move)

    if hit_ship["alive"]:
        await save_game(code)
        schedule_turn_timer(code)
        await send_boards(game, uid, f"🎯 Ранил ({coord_name})! Стреляй ещё.")
        await send_boards(game, opp_id, f"Соперник ранил ({coord_name}). Ждём его хода.")
        return

    # killed: auto-mark border as misses
    for c in hit_ship["orig"]:
        for n in neighbors(c):
            if n not in hit_ship["orig"] and n not in shooter["shots_hit"]:
                shooter["shots_miss"].add(n)
                opp["incoming_misses"].add(n)

    if all(not s["alive"] for s in opp["ships"]):
        log.info("game %s finished, winner=%s", code, uid)
        cancel_turn_timer(code)
        for pid in list(game["players"].keys()):
            user_game.pop(pid, None)
        games.pop(code, None)
        await delete_game(code)
        await send_boards(
            game, uid, f"💥 Убил ({coord_name})!\n🏆 ПОБЕДА!",
            reply_markup=kb_menu(),
        )
        await send_boards(
            game, opp_id,
            f"Соперник убил корабль {coord_name}.\n💀 Поражение.",
            reply_markup=kb_menu(),
        )
        return

    await save_game(code)
    schedule_turn_timer(code)
    await send_boards(game, uid, f"💥 Убил ({coord_name})! Стреляй ещё.")
    await send_boards(game, opp_id, f"Соперник убил корабль ({coord_name}). Ждём его хода.")


async def on_startup(dispatcher):
    global db_pool, BOT_USERNAME
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    log.info("db pool created")
    await load_state()
    try:
        me = await bot.get_me()
        BOT_USERNAME = me.username
        log.info("bot @%s", BOT_USERNAME)
    except Exception:
        log.exception("get_me failed (deep-link invites will not work)")
    try:
        await bot.set_my_commands([
            types.BotCommand("new", "Создать игру"),
            types.BotCommand("join", "Войти по коду"),
            types.BotCommand("replace", "Перекинуть расстановку"),
            types.BotCommand("ready", "Готов к бою"),
            types.BotCommand("surrender", "Сдаться"),
            types.BotCommand("help", "Помощь"),
        ])
    except Exception:
        log.exception("set_my_commands failed (non-fatal)")
    if STARTUP_DELAY_SEC > 0:
        log.info("startup delay %ss to release previous getUpdates session", STARTUP_DELAY_SEC)
        await asyncio.sleep(STARTUP_DELAY_SEC)
    try:
        await bot.delete_webhook(drop_pending_updates=False)
    except Exception:
        log.exception("delete_webhook failed (non-fatal)")


async def on_shutdown(dispatcher):
    if db_pool:
        await db_pool.close()


if __name__ == "__main__":
    executor.start_polling(
        dp,
        skip_updates=True,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
    )
