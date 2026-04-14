import os
import random
import string
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

API_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")

bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

FIELD = 10
LETTERS = "ABCDEFGHIJ"
FLEET = [4, 3, 3, 2, 2, 2, 1, 1, 1, 1]
CELL_UNKNOWN = "."
CELL_SHIP = "#"
CELL_HIT = "X"
CELL_MISS = "o"

# code -> game dict
games = {}
# user_id -> code
user_game = {}


def new_code():
    while True:
        code = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
        if code not in games:
            return code


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
    """Render a 10x10 field for a given player view.
    show_ships=True: own field (ships + incoming shots).
    show_ships=False: enemy field (your outgoing shots only).
    """
    # ASCII table keeps Telegram <pre> alignment predictable.
    header = "    " + "   ".join(LETTERS)
    separator = "   +" + "---+" * FIELD
    rows = [header, separator]
    for y in range(FIELD):
        row = [f"{y + 1:>2} |"]
        for x in range(FIELD):
            cell = (x, y)
            if show_ships:
                in_ship = any(cell in s for s in player["ships_cells"])
                hit = cell in player["incoming_hits"]
                miss = cell in player["incoming_misses"]
                if hit:
                    row.append(f" {CELL_HIT} |")
                elif miss:
                    row.append(f" {CELL_MISS} |")
                elif in_ship:
                    row.append(f" {CELL_SHIP} |")
                else:
                    row.append(f" {CELL_UNKNOWN} |")
            else:
                if cell in player["shots_hit"]:
                    row.append(f" {CELL_HIT} |")
                elif cell in player["shots_miss"]:
                    row.append(f" {CELL_MISS} |")
                else:
                    row.append(f" {CELL_UNKNOWN} |")
        rows.append("".join(row))
        rows.append(separator)
    return "<pre>" + "\n".join(rows) + "</pre>"


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


async def send_boards(game, user_id, prefix=""):
    p = game["players"][user_id]
    own = render(p, show_ships=True)
    enemy = render(p, show_ships=False)
    text = (
        f"{prefix}\n"
        f"🎯 Поле противника (твои выстрелы):\n{enemy}\n"
        f"🚢 Твоё поле:\n{own}"
    )
    await bot.send_message(user_id, text, parse_mode="HTML")


def other(game, user_id):
    return [u for u in game["players"] if u != user_id][0]


@dp.message_handler(commands=["start", "help"])
async def cmd_start(message: types.Message):
    await message.reply(
        "🚢 <b>Морской бой</b>\n\n"
        "/new — создать игру (получишь код)\n"
        "/join КОД — присоединиться по коду\n"
        "/replace — перекинуть расстановку\n"
        "/ready — готов к бою\n"
        "/surrender — сдаться\n\n"
        "Ход вводится координатой: <code>A1</code>, <code>B7</code>, <code>J10</code>",
        parse_mode="HTML",
    )


@dp.message_handler(commands=["new"])
async def cmd_new(message: types.Message):
    uid = message.from_user.id
    if uid in user_game:
        await message.reply("Ты уже в игре. /surrender чтобы выйти.")
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
    reroll(game["players"][uid])
    games[code] = game
    user_game[uid] = code
    await message.reply(
        f"🎲 Игра создана. Код: <code>{code}</code>\n"
        f"Отправь его сопернику — пусть напишет <code>/join {code}</code>\n\n"
        f"Пока можешь /replace — перекинуть расстановку.",
        parse_mode="HTML",
    )
    await send_boards(game, uid, "Твоя расстановка:")


@dp.message_handler(commands=["join"])
async def cmd_join(message: types.Message):
    uid = message.from_user.id
    if uid in user_game:
        await message.reply("Ты уже в игре. /surrender чтобы выйти.")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply("Формат: /join КОД")
        return
    code = parts[1].strip().upper()
    game = games.get(code)
    if not game:
        await message.reply("Игра с таким кодом не найдена.")
        return
    if game["state"] != "WAITING":
        await message.reply("Игра уже идёт или завершена.")
        return
    game["players"][uid] = new_player()
    reroll(game["players"][uid])
    user_game[uid] = code
    game["state"] = "PLACING"
    await message.reply(
        "✅ Присоединился. /replace — перекинуть расстановку, /ready — готов к бою."
    )
    await send_boards(game, uid, "Твоя расстановка:")
    await bot.send_message(game["host"], "🎮 Соперник подключился! Жми /ready когда готов.")


@dp.message_handler(commands=["replace"])
async def cmd_replace(message: types.Message):
    uid = message.from_user.id
    code = user_game.get(uid)
    if not code:
        await message.reply("Ты не в игре.")
        return
    game = games[code]
    if game["state"] not in ("WAITING", "PLACING"):
        await message.reply("Бой уже начался, расстановку менять нельзя.")
        return
    p = game["players"][uid]
    if p["ready"]:
        await message.reply("Ты уже нажал /ready.")
        return
    reroll(p)
    await send_boards(game, uid, "Новая расстановка:")


@dp.message_handler(commands=["ready"])
async def cmd_ready(message: types.Message):
    uid = message.from_user.id
    code = user_game.get(uid)
    if not code:
        await message.reply("Ты не в игре.")
        return
    game = games[code]
    if len(game["players"]) < 2:
        await message.reply("Ждём второго игрока.")
        return
    game["players"][uid]["ready"] = True
    await message.reply("✔ Готов.")
    opp = other(game, uid)
    if game["players"][opp]["ready"]:
        # start
        game["state"] = "PLAYING"
        game["turn"] = random.choice(list(game["players"].keys()))
        first = game["turn"]
        second = other(game, first)
        await bot.send_message(first, "🔫 Твой ход. Координата, например B7")
        await bot.send_message(second, "⏳ Ход соперника.")
    else:
        await bot.send_message(opp, "Соперник готов. Жми /ready когда расставишь корабли.")


@dp.message_handler(commands=["surrender"])
async def cmd_surrender(message: types.Message):
    uid = message.from_user.id
    code = user_game.get(uid)
    if not code:
        await message.reply("Ты не в игре.")
        return
    game = games[code]
    await message.reply("🏳 Ты сдался.")
    for pid in list(game["players"].keys()):
        user_game.pop(pid, None)
        if pid != uid:
            try:
                await bot.send_message(pid, "🏆 Соперник сдался. Победа!")
            except Exception:
                pass
    games.pop(code, None)


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
        await send_boards(game, uid, f"🌊 Мимо ({coord_name}). Ход соперника.")
        await send_boards(game, opp_id, f"Соперник стрелял {coord_name} — мимо. Твой ход.")
        return

    hit_ship["alive"].remove(move)
    shooter["shots_hit"].add(move)
    opp["incoming_hits"].add(move)

    if hit_ship["alive"]:
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
        await send_boards(game, uid, f"💥 Убил ({coord_name})!\n🏆 ПОБЕДА!")
        await send_boards(game, opp_id, f"Соперник убил {coord_name}.\n💀 Поражение.")
        for pid in list(game["players"].keys()):
            user_game.pop(pid, None)
        games.pop(code, None)
        return

    await send_boards(game, uid, f"💥 Убил ({coord_name})! Стреляй ещё.")
    await send_boards(game, opp_id, f"Соперник убил корабль ({coord_name}). Ждём его хода.")


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
