# bot.py (БЕЗ БД: всё хранится в data.json)
# Требования:
#   pip install aiogram
#
# .env (пример):
# BOT_TOKEN=...
# LOG_CHAT_ID=-1003610019728
# LOG_TOPIC_ID=3
# TEST_CHAT_ID=-1003610019728
# MAIN_CHAT_ID=-1003102382326

import asyncio
import time
import hashlib
import os
import re
import shlex
import json
import html
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Deque, Tuple, List, Any
from collections import deque, defaultdict

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    CallbackQuery,
    ChatPermissions,
    InlineKeyboardMarkup,
    BotCommand,
    BotCommandScopeDefault,
    BotCommandScopeChatAdministrators,
)
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ChatType
from aiogram.exceptions import TelegramBadRequest


# =========================
# .env loader (без python-dotenv)
# =========================
def load_dotenv(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except Exception:
        pass


load_dotenv()


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def env_int_optional(name: str) -> Optional[int]:
    v = os.getenv(name, "").strip()
    if not v:
        return None
    try:
        return int(v)
    except Exception:
        return None


# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is empty. Put it into .env")

# Логи (форум-топик)
LOG_CHAT_ID = env_int("LOG_CHAT_ID", -1003610019728)
LOG_TOPIC_ID = env_int("LOG_TOPIC_ID", 3)  # message_thread_id

# Чаты
TEST_CHAT_ID = env_int_optional("TEST_CHAT_ID")  # -1003610019728
MAIN_CHAT_ID = env_int_optional("MAIN_CHAT_ID")  # -1003102382326

# Файл данных (НЕ БД)
DATA_PATH = "data.json"

# Ссылки
LINK_MARKERS = ("http://", "https://", "t.me/", "www.")

# Пагинация
INACTIVE_PAGE_SIZE = 15
WL_PAGE_SIZE = 15

# GUI pages
PAGE_MAIN = "main"
PAGE_TEXT = "text"
PAGE_MEDIA = "media"
PAGE_CLEANUP = "cleanup"

# Сколько хранить активности (чтобы data.json не рос бесконечно)
ACTIVITY_KEEP_DAYS = 180
ACTIVITY_MAX_PER_CHAT = 20000


# =========================
# ROLES
# =========================
ROLE_SEEKER = "seeker"
ROLE_MOD = "moderator"
ROLE_ADMIN = "admin"
ROLE_HEAD = "head_admin"
ROLE_CREATOR = "creator"

ROLE_TITLES = {
    ROLE_CREATOR: "Создатель",
    ROLE_HEAD: "Руководитель Админов",
    ROLE_ADMIN: "Админ",
    ROLE_MOD: "Модератор",
    ROLE_SEEKER: "Ищет людей",
}

ROLE_ORDER = [ROLE_SEEKER, ROLE_MOD, ROLE_ADMIN, ROLE_HEAD, ROLE_CREATOR]
ROLE_RANK = {r: i for i, r in enumerate(ROLE_ORDER)}


def role_at_least(role: Optional[str], required: str) -> bool:
    if role is None:
        return False
    return ROLE_RANK.get(role, -1) >= ROLE_RANK[required]


def can_use(role: Optional[str], cmd: str) -> bool:
    if cmd == "invite":
        return role_at_least(role, ROLE_SEEKER)
    if cmd in ("warn", "mute", "unmute"):
        return role_at_least(role, ROLE_MOD)
    if cmd in ("ban", "unban"):
        return role_at_least(role, ROLE_ADMIN)
    if cmd in (
        "kick",
        "setrole",
        "delrole",
        "automute",
        "setrules",
        "setforum",
        "settings",
        "inactive",
    ):
        return role_at_least(role, ROLE_HEAD)
    if cmd == "to_main":
        return role_at_least(role, ROLE_MOD)
    return False


# =========================
# SETTINGS MODEL
# =========================
@dataclass
class ChatSettings:
    enabled: bool = True
    flood_limit: int = 6
    flood_window_sec: int = 10
    repeat_limit: int = 3
    block_links: bool = True

    sticker_mode: str = "limit"  # allow|limit|ban
    gif_mode: str = "limit"  # allow|limit|ban
    sticker_limit: int = 4
    gif_limit: int = 3
    media_window_sec: int = 12

    action: str = "mute"  # delete|mute
    mute_seconds: int = 14400  # 4 часа

    cleanup_enabled: bool = False
    cleanup_days: int = 14
    cleanup_mode: str = "kick"  # kick|ban


DEFAULT = ChatSettings()

ACTION_TITLE = {"delete": "Удалять", "mute": "Мут + удаление"}
MODE_TITLE = {"allow": "Разрешить", "limit": "Лимит", "ban": "Запрет"}


def action_title(x: str) -> str:
    return ACTION_TITLE.get(x, x)


def mode_title(x: str) -> str:
    return MODE_TITLE.get(x, x)


# =========================
# FILE STORAGE (data.json)
# =========================
_data_lock = asyncio.Lock()

DATA: Dict[str, Any] = {
    "settings": {},
    "meta": {},
    "roles": {},
    "warns": {},
    "activity": {},
    "whitelist": {},  # общий whitelist по чату
}


def h(s: Any) -> str:
    return html.escape("" if s is None else str(s), quote=True)


def _safe_int_key(k: Any) -> Optional[int]:
    try:
        return int(k)
    except Exception:
        return None


def _chat_key(chat_id: int) -> str:
    return str(int(chat_id))


def _user_key(user_id: int) -> str:
    return str(int(user_id))


async def load_data():
    global DATA
    async with _data_lock:
        if not os.path.exists(DATA_PATH):
            return
        try:
            with open(DATA_PATH, "r", encoding="utf-8") as f:
                obj = json.load(f)
            if isinstance(obj, dict):
                for k in DATA.keys():
                    if k in obj and isinstance(obj[k], dict):
                        DATA[k] = obj[k]
        except Exception:
            pass


async def save_data():
    async with _data_lock:
        tmp = DATA_PATH + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(DATA, f, ensure_ascii=False, indent=2)
            os.replace(tmp, DATA_PATH)
        except Exception:
            pass


def get_settings_local(chat_id: int) -> ChatSettings:
    ck = _chat_key(chat_id)
    raw = DATA["settings"].get(ck)
    if not isinstance(raw, dict):
        return ChatSettings(**asdict(DEFAULT))
    d = asdict(DEFAULT)
    for k, v in raw.items():
        if k in d:
            d[k] = v
    try:
        return ChatSettings(**d)
    except Exception:
        return ChatSettings(**asdict(DEFAULT))


async def set_setting_local(chat_id: int, field: str, value: Any):
    allowed = set(asdict(DEFAULT).keys())
    if field not in allowed:
        raise ValueError("Bad field")
    ck = _chat_key(chat_id)
    if ck not in DATA["settings"] or not isinstance(DATA["settings"][ck], dict):
        DATA["settings"][ck] = asdict(DEFAULT)
    DATA["settings"][ck][field] = value
    await save_data()


def get_meta_local(chat_id: int) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    ck = _chat_key(chat_id)
    m = DATA["meta"].get(ck)
    if not isinstance(m, dict):
        return None, None, None
    fc = _safe_int_key(m.get("forum_chat_id")) if m.get("forum_chat_id") is not None else None
    ft = _safe_int_key(m.get("forum_topic_id")) if m.get("forum_topic_id") is not None else None
    rt = m.get("rules_text")
    return fc, ft, rt if isinstance(rt, str) else None


async def set_meta_local(
    chat_id: int,
    forum_chat_id: Optional[int] = None,
    forum_topic_id: Optional[int] = None,
    rules_text: Optional[str] = None,
):
    ck = _chat_key(chat_id)
    if ck not in DATA["meta"] or not isinstance(DATA["meta"][ck], dict):
        DATA["meta"][ck] = {"forum_chat_id": None, "forum_topic_id": None, "rules_text": None}
    if forum_chat_id is not None:
        DATA["meta"][ck]["forum_chat_id"] = int(forum_chat_id)
    if forum_topic_id is not None:
        DATA["meta"][ck]["forum_topic_id"] = int(forum_topic_id)
    if rules_text is not None:
        DATA["meta"][ck]["rules_text"] = str(rules_text)
    await save_data()


def get_role_local(chat_id: int, user_id: int) -> Optional[str]:
    ck = _chat_key(chat_id)
    roles = DATA["roles"].get(ck)
    if not isinstance(roles, dict):
        return None
    r = roles.get(_user_key(user_id))
    return str(r) if isinstance(r, str) else None


async def set_role_local(chat_id: int, user_id: int, role: str):
    if role not in ROLE_RANK:
        raise ValueError("Bad role")
    ck = _chat_key(chat_id)
    if ck not in DATA["roles"] or not isinstance(DATA["roles"][ck], dict):
        DATA["roles"][ck] = {}
    DATA["roles"][ck][_user_key(user_id)] = role
    await save_data()


async def del_role_local(chat_id: int, user_id: int):
    ck = _chat_key(chat_id)
    roles = DATA["roles"].get(ck)
    if isinstance(roles, dict):
        roles.pop(_user_key(user_id), None)
    await save_data()


def list_roles_local(chat_id: int) -> List[Tuple[int, str]]:
    ck = _chat_key(chat_id)
    roles = DATA["roles"].get(ck)
    if not isinstance(roles, dict):
        return []
    out: List[Tuple[int, str]] = []
    for uid_s, r in roles.items():
        uid = _safe_int_key(uid_s)
        if uid is None or not isinstance(r, str):
            continue
        out.append((uid, r))
    return out


# =========================
# WHITELIST (общий для чата)
# =========================
def is_whitelisted(chat_id: int, user_id: int) -> bool:
    ck = _chat_key(chat_id)
    wl = DATA.get("whitelist", {}).get(ck)
    if not isinstance(wl, list):
        return False
    return str(int(user_id)) in set(map(str, wl))


async def whitelist_add(chat_id: int, user_id: int):
    ck = _chat_key(chat_id)
    if "whitelist" not in DATA or not isinstance(DATA["whitelist"], dict):
        DATA["whitelist"] = {}
    if ck not in DATA["whitelist"] or not isinstance(DATA["whitelist"][ck], list):
        DATA["whitelist"][ck] = []
    s = set(map(str, DATA["whitelist"][ck]))
    s.add(str(int(user_id)))
    DATA["whitelist"][ck] = sorted(s)
    await save_data()


async def whitelist_remove(chat_id: int, user_id: int):
    ck = _chat_key(chat_id)
    wl = DATA.get("whitelist", {}).get(ck)
    if isinstance(wl, list):
        DATA["whitelist"][ck] = [x for x in wl if str(x) != str(int(user_id))]
    await save_data()


def whitelist_list(chat_id: int) -> List[int]:
    ck = _chat_key(chat_id)
    wl = DATA.get("whitelist", {}).get(ck)
    if not isinstance(wl, list):
        return []
    out: List[int] = []
    for x in wl:
        try:
            out.append(int(x))
        except Exception:
            pass
    return out


# =========================
# Activity save debounce
# =========================
_activity_save_task: Optional[asyncio.Task] = None


def schedule_activity_save():
    global _activity_save_task
    if _activity_save_task and not _activity_save_task.done():
        return

    async def _delayed():
        await asyncio.sleep(5)
        await save_data()

    _activity_save_task = asyncio.create_task(_delayed())


async def upsert_activity_local(chat_id: int, user_id: int, ts: int, username: Optional[str]):
    ck = _chat_key(chat_id)
    if ck not in DATA["activity"] or not isinstance(DATA["activity"][ck], dict):
        DATA["activity"][ck] = {}
    ukey = _user_key(user_id)

    uname = (username or "").strip()
    if uname.startswith("@"):
        uname = uname[1:]
    uname = uname.lower() if uname else None

    DATA["activity"][ck][ukey] = {"last_ts": int(ts), "username": uname}
    schedule_activity_save()


def resolve_username_to_id_local(chat_id: int, username: str) -> Optional[int]:
    ck = _chat_key(chat_id)
    activity = DATA["activity"].get(ck)
    if not isinstance(activity, dict):
        return None
    uname = (username or "").strip()
    if uname.startswith("@"):
        uname = uname[1:]
    uname = uname.lower()
    if not uname:
        return None

    best_uid = None
    best_ts = -1
    for uid_s, info in activity.items():
        uid = _safe_int_key(uid_s)
        if uid is None or not isinstance(info, dict):
            continue
        if (info.get("username") or "") != uname:
            continue
        ts = info.get("last_ts")
        if isinstance(ts, int) and ts > best_ts:
            best_ts = ts
            best_uid = uid
    return best_uid


def count_inactive_local(chat_id: int, cutoff_ts: int) -> int:
    ck = _chat_key(chat_id)
    activity = DATA["activity"].get(ck)
    if not isinstance(activity, dict):
        return 0
    cnt = 0
    for _, info in activity.items():
        if isinstance(info, dict) and isinstance(info.get("last_ts"), int):
            if info["last_ts"] < cutoff_ts:
                cnt += 1
    return cnt


def fetch_inactive_local(chat_id: int, cutoff_ts: int, limit: int, offset: int) -> List[Tuple[int, int]]:
    ck = _chat_key(chat_id)
    activity = DATA["activity"].get(ck)
    if not isinstance(activity, dict):
        return []

    rows: List[Tuple[int, int]] = []
    for uid_s, info in activity.items():
        uid = _safe_int_key(uid_s)
        if uid is None or not isinstance(info, dict):
            continue
        ts = info.get("last_ts")
        if isinstance(ts, int) and ts < cutoff_ts:
            rows.append((uid, ts))
    rows.sort(key=lambda x: x[1])
    return rows[offset : offset + limit]


async def add_warn_local(chat_id: int, user_id: int, by_id: int, reason: str) -> int:
    ck = _chat_key(chat_id)
    if ck not in DATA["warns"] or not isinstance(DATA["warns"][ck], dict):
        DATA["warns"][ck] = {}
    ukey = _user_key(user_id)

    entry = DATA["warns"][ck].get(ukey)
    now = int(time.time())

    if not isinstance(entry, dict):
        entry = {"count": 0, "last_ts": now, "last_reason": "", "last_by": by_id}

    entry["count"] = int(entry.get("count", 0)) + 1
    entry["last_ts"] = now
    entry["last_reason"] = str(reason)
    entry["last_by"] = int(by_id)

    DATA["warns"][ck][ukey] = entry
    await save_data()
    return int(entry["count"])


# =========================
# Periodic activity cleanup
# =========================
async def prune_activity_once():
    cutoff = int(time.time()) - ACTIVITY_KEEP_DAYS * 86400
    changed = False

    activity_all = DATA.get("activity")
    if not isinstance(activity_all, dict):
        return

    for _, users in list(activity_all.items()):
        if not isinstance(users, dict):
            continue

        items: List[Tuple[str, int]] = []
        for uid_s, info in users.items():
            if not isinstance(info, dict):
                continue
            ts = info.get("last_ts")
            if isinstance(ts, int):
                items.append((uid_s, ts))

        items.sort(key=lambda x: x[1], reverse=True)

        keep_keys = set()
        for uid_s, ts in items:
            if ts >= cutoff:
                keep_keys.add(uid_s)

        for uid_s, _ in items[:ACTIVITY_MAX_PER_CHAT]:
            keep_keys.add(uid_s)

        if len(keep_keys) != len(users):
            for uid_s in list(users.keys()):
                if uid_s not in keep_keys:
                    users.pop(uid_s, None)
                    changed = True

    if changed:
        await save_data()


async def prune_activity_loop():
    while True:
        try:
            await prune_activity_once()
        except Exception:
            pass
        await asyncio.sleep(24 * 3600)


# =========================
# IN-MEMORY SPAM STATE
# =========================
msg_times: Dict[int, Dict[int, Deque[float]]] = defaultdict(lambda: defaultdict(deque))
last_hash: Dict[int, Dict[int, Tuple[str, int]]] = defaultdict(lambda: defaultdict(lambda: ("", 0)))
sticker_times: Dict[int, Dict[int, Deque[float]]] = defaultdict(lambda: defaultdict(deque))
gif_times: Dict[int, Dict[int, Deque[float]]] = defaultdict(lambda: defaultdict(deque))

# album(media_group) fix: помнить группы, чтобы не считать как флуд/gif
album_seen: Dict[int, Dict[int, Dict[str, float]]] = defaultdict(lambda: defaultdict(dict))


# =========================
# HELPERS
# =========================
def norm_text(s: str) -> str:
    s = (s or "").strip().lower()
    while "  " in s:
        s = s.replace("  ", " ")
    return s


def text_hash(s: str) -> str:
    s = norm_text(s)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def contains_link(text: str) -> bool:
    t = (text or "").lower()
    if any(m in t for m in LINK_MARKERS):
        return True
    if re.search(r"(https?://|t\.me/|www\.)\S+", t):
        return True
    return False


def format_duration(sec: Optional[int]) -> str:
    if not sec:
        return "—"
    s = int(sec)
    d, s = divmod(s, 86400)
    h_, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    parts = []
    if d:
        parts.append(f"{d}д")
    if h_:
        parts.append(f"{h_}ч")
    if m:
        parts.append(f"{m}м")
    if s and not parts:
        parts.append(f"{s}с")
    return " ".join(parts) if parts else "0с"


def parse_duration_to_seconds(s: str) -> Optional[int]:
    s = (s or "").strip().lower()
    if not s:
        return None
    if s.isdigit():
        return int(s)

    s = s.replace("сек", "s").replace("с", "s")
    s = s.replace("мин", "m").replace("м", "m")
    s = s.replace("час", "h").replace("ч", "h")
    s = s.replace("дн", "d").replace("д", "d")

    pattern = r"(\d+)\s*([smhd])"
    total = 0
    found = False
    for num, unit in re.findall(pattern, s):
        found = True
        n = int(num)
        if unit == "s":
            total += n
        elif unit == "m":
            total += n * 60
        elif unit == "h":
            total += n * 3600
        elif unit == "d":
            total += n * 86400
    if not found:
        return None
    return total


def split_command_args(text: str) -> List[str]:
    if not text:
        return []
    try:
        parts = shlex.split(text)
    except ValueError:
        parts = text.split()
    if not parts:
        return []
    parts[0] = parts[0].split("@", 1)[0]  # /cmd@bot -> /cmd
    return parts


def mention_html(user_id: int, label: str) -> str:
    lbl = (label or str(user_id)).strip()
    return f'<a href="tg://user?id={int(user_id)}">{h(lbl)}</a>'


async def display_user_mention(bot: Bot, chat_id: int, user_id: int) -> str:
    try:
        m = await bot.get_chat_member(chat_id, user_id)
        if m.user.username:
            return mention_html(user_id, f"@{m.user.username}")
        if m.user.full_name:
            return mention_html(user_id, m.user.full_name)
    except TelegramBadRequest:
        pass
    return mention_html(user_id, str(user_id))


async def get_effective_role(bot: Bot, chat_id: int, user_id: int) -> Optional[str]:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        if member.status == "creator":
            return ROLE_CREATOR
    except TelegramBadRequest:
        pass
    return get_role_local(chat_id, user_id)


async def resolve_target_user_id(message: Message) -> Optional[int]:
    if message.reply_to_message and message.reply_to_message.from_user:
        return message.reply_to_message.from_user.id

    parts = split_command_args(message.text or "")
    if len(parts) < 2:
        return None

    target = parts[1].strip()
    if target.isdigit():
        return int(target)
    if target.startswith("@"):
        return resolve_username_to_id_local(message.chat.id, target)
    return None


async def ensure_can_moderate_target(bot: Bot, chat_id: int, actor_id: int, target_id: int) -> bool:
    try:
        target = await bot.get_chat_member(chat_id, target_id)
        if target.status in ("creator", "administrator"):
            return False
    except TelegramBadRequest:
        pass

    actor_role = await get_effective_role(bot, chat_id, actor_id)
    target_role = await get_effective_role(bot, chat_id, target_id)
    if actor_role and target_role:
        if ROLE_RANK.get(target_role, -1) >= ROLE_RANK.get(actor_role, -1):
            return False

    return True


# =========================
# LOGGING TO FORUM
# =========================
async def log_action(bot: Bot, chat_id: int, text_html: str):
    forum_chat_id, forum_topic_id, _ = get_meta_local(chat_id)
    fc = forum_chat_id or LOG_CHAT_ID
    ft = forum_topic_id or LOG_TOPIC_ID
    try:
        await bot.send_message(
            fc,
            text_html,
            message_thread_id=ft,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except TelegramBadRequest:
        pass


# =========================
# ACTIONS (automod) + LOG
# =========================
async def apply_action(bot: Bot, message: Message, settings: ChatSettings, reason: str):
    # delete message
    try:
        await message.delete()
    except TelegramBadRequest:
        pass

    # log automod
    try:
        who = await display_user_mention(bot, message.chat.id, message.from_user.id)
        txt = (message.text or message.caption or "").strip()
        if len(txt) > 180:
            txt = txt[:180] + "…"
        extra = ""
        if settings.action == "mute":
            extra = f"\nСрок: <code>{int(settings.mute_seconds)}</code> сек"

        await log_action(
            bot,
            message.chat.id,
            "🤖 <b>AUTO-MOD</b>\n"
            f"Чат: <code>{message.chat.id}</code>\n"
            f"Кого: {who}\n"
            f"Причина: <code>{h(reason)}</code>\n"
            f"Действие: <code>{h(settings.action)}</code>"
            f"{extra}\n"
            + (f"Текст: <code>{h(txt)}</code>" if txt else "Тип: <code>media</code>"),
        )
    except Exception:
        pass

    # mute
    if settings.action == "mute":
        until = int(time.time()) + int(settings.mute_seconds)
        perms = ChatPermissions(can_send_messages=False)
        try:
            await bot.restrict_chat_member(
                chat_id=message.chat.id,
                user_id=message.from_user.id,
                permissions=perms,
                until_date=until,
            )
        except TelegramBadRequest:
            pass


# =========================
# COMMAND HINTS (подсказки команд)
# =========================
async def setup_bot_commands(bot: Bot, chat_ids: List[int]):
    # Telegram API меню принимает только латиницу/цифры/_
    try:
        await bot.set_my_commands(
            commands=[
                BotCommand(command="commands", description="Список команд бота"),
                BotCommand(command="rules", description="Правила чата"),
                BotCommand(command="admins", description="Список администрации (по ролям)"),
            ],
            scope=BotCommandScopeDefault(),
            language_code="ru",
        )
    except TelegramBadRequest:
        pass

    admin_commands = [
        BotCommand(command="to_main", description="Перенести (reply) в основную"),
        BotCommand(command="mute", description="Мут (reply/@username)"),
        BotCommand(command="unmute", description="Размут (reply/@username)"),
        BotCommand(command="warn", description="Warn (reply/@username)"),
        BotCommand(command="ban", description="Бан (reply/@username)"),
        BotCommand(command="unban", description="Разбан (reply/@username)"),
        BotCommand(command="kick", description="Кик (reply/@username)"),
        BotCommand(command="invite", description="Ссылка приглашения"),
        BotCommand(command="setrules", description="Изменить правила"),
        BotCommand(command="automute", description="Изменить авто-мут"),
        BotCommand(command="setrole", description="Назначить роль (reply)"),
        BotCommand(command="delrole", description="Снять роль (reply)"),
        BotCommand(command="setforum", description="Настроить чат логов"),
        BotCommand(command="settings", description="Настройки антиспама"),
        BotCommand(command="inactive", description="Неактивные участники"),
        BotCommand(command="wl_add", description="Whitelist: добавить (reply)"),
        BotCommand(command="wl_del", description="Whitelist: убрать (reply)"),
        BotCommand(command="wl_list", description="Whitelist: список"),
    ]

    for chat_id in chat_ids:
        try:
            await bot.set_my_commands(
                commands=admin_commands,
                scope=BotCommandScopeChatAdministrators(chat_id=chat_id),
                language_code="ru",
            )
        except TelegramBadRequest:
            pass


# =========================
# UI TEXT (settings) — HTML
# =========================
def settings_text(s: ChatSettings, page: str) -> str:
    header = {
        PAGE_MAIN: "🛡 <b>Основное</b>",
        PAGE_TEXT: "💬 <b>Антиспам текста</b>",
        PAGE_MEDIA: "🎞 <b>Стикеры / GIF</b>",
        PAGE_CLEANUP: "🧹 <b>Неактивные участники</b>",
    }.get(page, "⚙️ <b>Настройки</b>")

    lines: List[str] = [header, ""]

    if page == PAGE_MAIN:
        lines += [
            f"• Модерация: <b>{'ON' if s.enabled else 'OFF'}</b>",
            f"• Ссылки: <b>{'ON' if s.block_links else 'OFF'}</b>",
            "",
            f"• Действие: <b>{h(action_title(s.action))}</b>",
        ]
        if s.action == "mute":
            lines.append(f"• Авто-мут: <b>{h(format_duration(s.mute_seconds))}</b> (<code>{s.mute_seconds}</code>с)")
        lines += [
            "",
            "<i>Можно командой:</i>",
            "<code>/automute 2h30m</code>",
            "",
            "<i>Whitelist:</i> пользователи, которых автомод не трогает.",
        ]

    elif page == PAGE_TEXT:
        lines += [
            f"• Антифлуд: <b>{s.flood_limit}</b> сообщений / <b>{s.flood_window_sec}</b> сек",
            f"• Антиповтор: <b>{s.repeat_limit}</b> одинаковых подряд",
        ]

    elif page == PAGE_MEDIA:
        sticker_line = f"• Стикеры: <b>{h(mode_title(s.sticker_mode))}</b>"
        if s.sticker_mode == "limit":
            sticker_line += f" (лимит {s.sticker_limit}/{s.media_window_sec}с)"
        gif_line = f"• GIF/Видео: <b>{h(mode_title(s.gif_mode))}</b>"
        if s.gif_mode == "limit":
            gif_line += f" (лимит {s.gif_limit}/{s.media_window_sec}с)"
        lines += [
            sticker_line,
            gif_line,
            f"• Окно медиа: <b>{s.media_window_sec}</b> сек",
            "",
            "<i>Примечание:</i> альбомы (несколько фото одним сообщением) не считаются как флуд/гиф-спам.",
        ]

    elif page == PAGE_CLEANUP:
        lines += [
            f"• Авто-очистка: <b>{'ON' if s.cleanup_enabled else 'OFF'}</b>",
            f"• Порог: <b>{s.cleanup_days}</b> дней",
            f"• Режим: <b>{h(s.cleanup_mode)}</b>",
            "",
            "<i>Список строится по тем, кого бот видел (писали после установки бота).</i>",
        ]

    return "\n".join(lines)


# =========================
# UI KEYBOARDS (Tabbed)
# =========================
def nav_row(current: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=("🛡 Основное" if current != PAGE_MAIN else "✅ 🛡 Основное"), callback_data=f"ui:page:{PAGE_MAIN}")
    kb.button(text=("💬 Текст" if current != PAGE_TEXT else "✅ 💬 Текст"), callback_data=f"ui:page:{PAGE_TEXT}")
    kb.button(text=("🎞 Медиа" if current != PAGE_MEDIA else "✅ 🎞 Медиа"), callback_data=f"ui:page:{PAGE_MEDIA}")
    kb.button(text=("🧹 Неактив" if current != PAGE_CLEANUP else "✅ 🧹 Неактив"), callback_data=f"ui:page:{PAGE_CLEANUP}")
    kb.adjust(2, 2)
    return kb


def build_kb_main(s: ChatSettings):
    kb = InlineKeyboardBuilder()
    kb.button(text=f"Модерация: {'ON' if s.enabled else 'OFF'}", callback_data="tg:toggle_enabled")
    kb.button(text=f"Ссылки: {'ON' if s.block_links else 'OFF'}", callback_data="tg:toggle_links")

    kb.button(text=f"Действие: {action_title(s.action)}", callback_data="tg:action_toggle")
    kb.button(text="Auto-mute = 4ч", callback_data="tg:mute_4h")

    kb.button(text="Auto-mute +30с", callback_data="tg:mute_plus30")
    kb.button(text="Auto-mute -30с", callback_data="tg:mute_minus30")

    kb.button(text="👥 Whitelist", callback_data="tg:wl_list:0")
    kb.adjust(2, 2, 2, 1)
    return kb


def build_kb_text(s: ChatSettings):
    kb = InlineKeyboardBuilder()
    kb.button(text=f"Антифлуд: {s.flood_limit}  (+)", callback_data="tg:flood_inc")
    kb.button(text=f"Антифлуд: {s.flood_limit}  (-)", callback_data="tg:flood_dec")

    kb.button(text=f"Окно: {s.flood_window_sec}с (+)", callback_data="tg:window_inc")
    kb.button(text=f"Окно: {s.flood_window_sec}с (-)", callback_data="tg:window_dec")

    kb.button(text=f"Повторы: {s.repeat_limit}  (+)", callback_data="tg:repeat_inc")
    kb.button(text=f"Повторы: {s.repeat_limit}  (-)", callback_data="tg:repeat_dec")

    kb.adjust(2, 2, 2)
    return kb


def build_kb_media(s: ChatSettings):
    kb = InlineKeyboardBuilder()
    kb.button(text=f"Стикеры: {mode_title(s.sticker_mode)}", callback_data="tg:sticker_mode")
    kb.button(text=f"GIF/Видео: {mode_title(s.gif_mode)}", callback_data="tg:gif_mode")

    kb.button(text=f"Стикер лимит: {s.sticker_limit} (+)", callback_data="tg:sticker_lim_inc")
    kb.button(text=f"Стикер лимит: {s.sticker_limit} (-)", callback_data="tg:sticker_lim_dec")

    kb.button(text=f"GIF лимит: {s.gif_limit} (+)", callback_data="tg:gif_lim_inc")
    kb.button(text=f"GIF лимит: {s.gif_limit} (-)", callback_data="tg:gif_lim_dec")

    kb.button(text=f"Окно медиа: {s.media_window_sec}с (+)", callback_data="tg:media_window_inc")
    kb.button(text=f"Окно медиа: {s.media_window_sec}с (-)", callback_data="tg:media_window_dec")

    kb.adjust(2, 2, 2, 2)
    return kb


def build_kb_cleanup(s: ChatSettings):
    kb = InlineKeyboardBuilder()
    kb.button(text=f"Авто-очистка: {'ON' if s.cleanup_enabled else 'OFF'}", callback_data="tg:cleanup_toggle")
    kb.button(text=f"Порог: {s.cleanup_days} дней", callback_data="tg:cleanup_days_cycle")

    kb.button(text=f"Режим: {s.cleanup_mode}", callback_data="tg:cleanup_mode")
    kb.button(text="📋 Список неактивных", callback_data="tg:inactive_list:0")
    kb.button(text="🧹 Запустить сейчас", callback_data="tg:cleanup_run_now")

    kb.adjust(2, 2, 1)
    return kb


def build_settings_markup(s: ChatSettings, page: str) -> InlineKeyboardMarkup:
    nav = nav_row(page)

    if page == PAGE_MAIN:
        section = build_kb_main(s)
    elif page == PAGE_TEXT:
        section = build_kb_text(s)
    elif page == PAGE_MEDIA:
        section = build_kb_media(s)
    elif page == PAGE_CLEANUP:
        section = build_kb_cleanup(s)
    else:
        section = build_kb_main(s)

    footer = InlineKeyboardBuilder()
    footer.button(text="🔄 Обновить", callback_data=f"ui:page:{page}")
    footer.button(text="🏠 В основное", callback_data=f"ui:page:{PAGE_MAIN}")
    footer.adjust(2)

    nav.attach(section)
    nav.attach(footer)
    return nav.as_markup()


# =========================
# INACTIVE LIST UI
# =========================
def build_inactive_kb(page: int, total: int):
    kb = InlineKeyboardBuilder()
    prev_page = page - 1
    next_page = page + 1
    max_page = max(0, (total - 1) // INACTIVE_PAGE_SIZE)

    if page > 0:
        kb.button(text="⬅️ Назад", callback_data=f"tg:inactive_list:{prev_page}")
    if page < max_page:
        kb.button(text="Вперёд ➡️", callback_data=f"tg:inactive_list:{next_page}")

    kb.button(text="🔙 В раздел 🧹", callback_data=f"ui:page:{PAGE_CLEANUP}")
    kb.adjust(2)
    return kb.as_markup()


async def render_inactive_list(call: CallbackQuery, bot: Bot, chat_id: int, page: int):
    s = get_settings_local(chat_id)
    cutoff = int(time.time()) - int(s.cleanup_days) * 24 * 3600

    total = count_inactive_local(chat_id, cutoff)
    offset = page * INACTIVE_PAGE_SIZE
    rows = fetch_inactive_local(chat_id, cutoff, INACTIVE_PAGE_SIZE, offset)

    lines = [
        "📋 <b>Неактивные участники</b>",
        f"<i>Критерий: не писали {s.cleanup_days} дней (учитываются только те, кого бот видел).</i>",
        "",
        f"Всего: <b>{total}</b> | Страница: <b>{page + 1}</b>",
        "—",
    ]

    if total == 0:
        lines.append("Пока никого нет ✅")
        kb = build_inactive_kb(page=0, total=0)
        await call.message.edit_text("\n".join(lines), reply_markup=kb, parse_mode="HTML")
        return

    for user_id, last_ts in rows:
        days_ago = int((time.time() - last_ts) // 86400)
        note = ""
        try:
            member = await bot.get_chat_member(chat_id, user_id)
            if member.status in ("left", "kicked"):
                note = " (уже не в чате)"
            if member.status in ("administrator", "creator"):
                note += " (админ)"
        except TelegramBadRequest:
            note = " (недоступно)"

        u = await display_user_mention(bot, chat_id, user_id)
        lines.append(f"• {u} — <b>{days_ago}</b> дн. назад{h(note)}")

    kb = build_inactive_kb(page=page, total=total)
    await call.message.edit_text("\n".join(lines), reply_markup=kb, parse_mode="HTML")


# =========================
# WHITELIST UI
# =========================
def build_wl_kb(page: int, total: int):
    kb = InlineKeyboardBuilder()
    max_page = max(0, (total - 1) // WL_PAGE_SIZE)

    if page > 0:
        kb.button(text="⬅️ Назад", callback_data=f"tg:wl_list:{page-1}")
    if page < max_page:
        kb.button(text="Вперёд ➡️", callback_data=f"tg:wl_list:{page+1}")

    kb.button(text="➕ Добавить (reply) /wl_add", callback_data="tg:wl_hint_add")
    kb.button(text="➖ Удалить (reply) /wl_del", callback_data="tg:wl_hint_del")
    kb.button(text="🔙 В настройки", callback_data=f"ui:page:{PAGE_MAIN}")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


async def render_wl_list(call: CallbackQuery, bot: Bot, chat_id: int, page: int):
    ids = whitelist_list(chat_id)
    total = len(ids)
    page = max(0, page)
    offset = page * WL_PAGE_SIZE
    chunk = ids[offset : offset + WL_PAGE_SIZE]

    lines = [
        "👥 <b>Whitelist</b>",
        "<i>Эти пользователи полностью исключены из автомода.</i>",
        "",
        f"Всего: <b>{total}</b> | Страница: <b>{page + 1}</b>",
        "—",
    ]

    if total == 0:
        lines.append("Пусто ✅")
    else:
        for uid in chunk:
            lines.append(f"• {await display_user_mention(bot, chat_id, uid)}")

    kb = build_wl_kb(page=page, total=total)
    await call.message.edit_text("\n".join(lines), reply_markup=kb, parse_mode="HTML")


# =========================
# CLEANUP LOGIC
# =========================
async def run_cleanup_once(bot: Bot, chat_id: int) -> Tuple[int, int]:
    s = get_settings_local(chat_id)
    cutoff = int(time.time()) - int(s.cleanup_days) * 24 * 3600

    rows = fetch_inactive_local(chat_id, cutoff, limit=10_000, offset=0)
    processed = 0
    removed = 0

    for user_id, _ in rows:
        processed += 1
        try:
            member = await bot.get_chat_member(chat_id, user_id)
            if member.status in ("administrator", "creator", "left", "kicked"):
                continue

            if s.cleanup_mode == "ban":
                await bot.ban_chat_member(chat_id, user_id)
            else:
                await bot.ban_chat_member(chat_id, user_id)
                await bot.unban_chat_member(chat_id, user_id)

            removed += 1
        except TelegramBadRequest:
            continue

    return processed, removed


async def cleanup_loop(bot: Bot):
    while True:
        chats: List[int] = []
        for ck in (DATA.get("settings") or {}).keys():
            try:
                cid = int(ck)
            except Exception:
                continue
            s = get_settings_local(cid)
            if s.cleanup_enabled:
                chats.append(cid)

        for chat_id in chats:
            try:
                await run_cleanup_once(bot, chat_id)
            except Exception:
                pass

        await asyncio.sleep(24 * 3600)


# =========================
# DISPATCHER
# =========================
dp = Dispatcher()


async def render_settings(
    bot: Bot,
    chat_id: int,
    page: str = PAGE_MAIN,
    target_message: Optional[Message] = None,
    edit_cb: Optional[CallbackQuery] = None,
):
    s = get_settings_local(chat_id)
    text = settings_text(s, page)
    kb = build_settings_markup(s, page)

    if edit_cb:
        try:
            await edit_cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        except TelegramBadRequest:
            pass
        return

    if target_message:
        await target_message.answer(text, reply_markup=kb, parse_mode="HTML")


# =========================
# PUBLIC COMMANDS
# =========================
@dp.message(Command("commands"))
@dp.message(F.text.regexp(r"^/(команды)(@[\w_]+)?(\s|$)"))
async def cmd_commands(message: Message):
    txt = (
        "📚 <b>Команды бота</b>\n\n"
        "<b>Для всех:</b>\n"
        "• /admins — список администрации (по ролям)\n"
        "• /rules — правила чата (алиас: /правила)\n"
        "• /commands — список команд (алиас: /команды)\n\n"
        "<b>Ищет людей:</b>\n"
        "• /invite — одноразовая ссылка в основную группу (только из тестовой)\n\n"
        "<b>Модератор:</b>\n"
        "• /warn (reply/@username) причина\n"
        "• /mute (reply/@username) &lt;время&gt; причина\n"
        "• /unmute (reply/@username)\n"
        "• /to_main (reply) — перенести сообщение в основную (алиас: /перенести)\n\n"
        "<b>Админ:</b>\n"
        "• /ban (reply/@username) [время] причина\n"
        "• /unban (reply/@username)\n\n"
        "<b>Руководитель Админов / Создатель:</b>\n"
        "• /kick (reply/@username) причина\n"
        "• /setrole (reply) <seeker|moderator|admin|head_admin|creator>\n"
        "• /delrole (reply)\n"
        "• /automute <время>\n"
        "• /setrules <текст> (или reply)\n"
        "• /setforum <chat_id> <topic_id>\n"
        "• /settings — настройки антиспама\n"
        "• /inactive — неактивные участники\n"
        "• /wl_add /wl_del /wl_list — whitelist (только Создатель/Руководитель)\n\n"
        "<i>Поддерживаются кавычки:</i>\n"
        "<code>/mute \"@UserName\" \"10m\" \"причина\"</code>\n"
        "<code>/перенести --del</code>\n\n"
        "<i>Примечание:</i> меню Telegram показывает команды латиницей, но русские алиасы тоже работают."
    )
    await message.answer(txt, parse_mode="HTML", disable_web_page_preview=True)


@dp.message(Command("rules"))
@dp.message(F.text.regexp(r"^/(правила)(@[\w_]+)?(\s|$)"))
async def cmd_rules(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    _, _, rules = get_meta_local(message.chat.id)
    if not rules:
        rules = "Правила ещё не настроены."
    await message.answer(f"📌 <b>Правила чата</b>\n\n{h(rules)}", parse_mode="HTML")


@dp.message(F.new_chat_members)
async def on_new_members(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    _, _, rules = get_meta_local(message.chat.id)
    if not rules:
        return
    await message.answer(f"👋 Добро пожаловать!\n📌 <b>Правила чата:</b>\n\n{h(rules)}", parse_mode="HTML")


@dp.message(Command("admins"))
async def cmd_admins(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Команда работает в группах/супергруппах.")
        return

    rows = list_roles_local(message.chat.id)
    by_role: Dict[str, List[int]] = {r: [] for r in ROLE_ORDER}
    for uid, r in rows:
        if r in by_role:
            by_role[r].append(int(uid))

    # creator из Telegram
    try:
        admins = await bot.get_chat_administrators(message.chat.id)
        for m in admins:
            if m.status == "creator":
                if m.user.id not in by_role[ROLE_CREATOR]:
                    by_role[ROLE_CREATOR].append(m.user.id)
    except TelegramBadRequest:
        pass

    lines = ["👮 <b>Администрация</b>", ""]
    any_added = False
    for r in reversed(ROLE_ORDER):
        users = by_role.get(r, [])
        if not users:
            continue
        any_added = True
        lines.append(f"<b>{h(ROLE_TITLES[r])}</b>")
        for uid in users:
            lines.append(f"• {await display_user_mention(bot, message.chat.id, uid)}")
        lines.append("")

    if not any_added:
        lines.append("<i>Пока роли не назначены.</i>")

    await message.answer("\n".join(lines), parse_mode="HTML")


# =========================
# STAFF / MANAGEMENT COMMANDS
# =========================
@dp.message(Command("setforum"))
async def cmd_setforum(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP) or not message.from_user:
        return

    role = await get_effective_role(bot, message.chat.id, message.from_user.id)
    if not can_use(role, "setforum"):
        await message.answer("⛔ Недостаточно прав.")
        return

    parts = split_command_args(message.text or "")
    if len(parts) < 3:
        await message.answer("Использование: /setforum <chat_id> <topic_id>\nПример: /setforum -1003610019728 3")
        return

    try:
        fc = int(parts[1])
        ft = int(parts[2])
    except ValueError:
        await message.answer("Нужны числа: /setforum -100... 3")
        return

    await set_meta_local(message.chat.id, forum_chat_id=fc, forum_topic_id=ft)
    await message.answer("✅ Форум-лог установлен для этого чата.")


@dp.message(Command("setrules"))
async def cmd_setrules(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP) or not message.from_user:
        return

    role = await get_effective_role(bot, message.chat.id, message.from_user.id)
    if not can_use(role, "setrules"):
        await message.answer("⛔ Недостаточно прав.")
        return

    parts = split_command_args(message.text or "")
    text = ""
    if len(parts) >= 2:
        text = (message.text or "").split(maxsplit=1)[1].strip()
    if not text and message.reply_to_message:
        text = (message.reply_to_message.text or "").strip()
    if not text:
        await message.answer("Использование: /setrules <текст>\nили ответь на сообщение с текстом и напиши /setrules")
        return

    await set_meta_local(message.chat.id, rules_text=text)
    await message.answer("✅ Правила обновлены.")

    who = await display_user_mention(bot, message.chat.id, message.from_user.id)
    await log_action(bot, message.chat.id, f"📌 <b>RULES UPDATED</b>\nКто: {who}\nЧат: <code>{message.chat.id}</code>")


@dp.message(Command("automute"))
async def cmd_automute(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP) or not message.from_user:
        return

    role = await get_effective_role(bot, message.chat.id, message.from_user.id)
    if not can_use(role, "automute"):
        await message.answer("⛔ Недостаточно прав.")
        return

    parts = split_command_args(message.text or "")
    if len(parts) < 2:
        await message.answer("Использование: /automute 4h\nПримеры: 30m, 2h, 2ч30м, 1d")
        return

    sec = parse_duration_to_seconds(parts[1])
    if not sec or sec < 30 or sec > 86400:
        await message.answer("Некорректное время (30с…86400с).")
        return

    await set_setting_local(message.chat.id, "action", "mute")
    await set_setting_local(message.chat.id, "mute_seconds", sec)

    await message.answer(
        f"✅ Авто-мут установлен: <b>{h(format_duration(sec))}</b> (<code>{sec}</code>с).",
        parse_mode="HTML",
    )

    who = await display_user_mention(bot, message.chat.id, message.from_user.id)
    await log_action(
        bot,
        message.chat.id,
        f"⏱ <b>AUTOMUTE</b>\nКто: {who}\nЧат: <code>{message.chat.id}</code>\nЗначение: <code>{sec}</code> сек",
    )


@dp.message(Command("invite"))
async def cmd_invite(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP) or not message.from_user:
        return

    role = await get_effective_role(bot, message.chat.id, message.from_user.id)
    if not can_use(role, "invite"):
        await message.answer("⛔ Недостаточно прав.")
        return

    who = await display_user_mention(bot, message.chat.id, message.from_user.id)

    # seeker: одноразовая ссылка в основную, создаётся только из тестовой
    if role == ROLE_SEEKER:
        if TEST_CHAT_ID is None or MAIN_CHAT_ID is None:
            await message.answer("⚠️ TEST_CHAT_ID / MAIN_CHAT_ID не настроены в .env")
            return
        if message.chat.id != TEST_CHAT_ID:
            await message.answer(
                "⛔ Для роли <b>Ищет людей</b> ссылка в основную создаётся только в тестовой группе.",
                parse_mode="HTML",
            )
            return

        try:
            link = await bot.create_chat_invite_link(
                chat_id=MAIN_CHAT_ID,
                name=f"main-invite by seeker {message.from_user.id}",
                member_limit=1,
            )
            await message.answer(f"🔗 Одноразовая ссылка в основную группу:\n{h(link.invite_link)}", parse_mode="HTML")

            await log_action(
                bot,
                message.chat.id,
                f"🔗 <b>INVITE LINK (ONE-TIME → MAIN)</b>\n"
                f"Кто: {who}\n"
                f"Откуда: <code>{message.chat.id}</code> (test)\n"
                f"Куда: <code>{MAIN_CHAT_ID}</code> (main)\n"
                f"Лимит: <code>1</code>",
            )
        except TelegramBadRequest:
            await message.answer("Не удалось создать ссылку (нет прав у бота в основной группе).")
        return

    # остальные: ссылка в текущий чат
    try:
        link = await bot.create_chat_invite_link(message.chat.id, name=f"invite by {message.from_user.id}")
        await message.answer(f"🔗 Ссылка для приглашения:\n{h(link.invite_link)}", parse_mode="HTML")
        await log_action(bot, message.chat.id, f"🔗 <b>INVITE LINK</b>\nКто: {who}\nЧат: <code>{message.chat.id}</code>")
    except TelegramBadRequest:
        await message.answer("Не удалось создать ссылку (нет прав у бота).")


@dp.message(Command("setrole"))
async def cmd_setrole(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP) or not message.from_user:
        return

    role_me = await get_effective_role(bot, message.chat.id, message.from_user.id)
    if not can_use(role_me, "setrole"):
        await message.answer("⛔ Недостаточно прав.")
        return

    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.answer("Использование: ответь на сообщение пользователя и напиши: /setrole moderator")
        return

    parts = split_command_args(message.text or "")
    if len(parts) < 2:
        await message.answer("Роли: seeker | moderator | admin | head_admin | creator")
        return

    new_role = parts[1].strip()
    if new_role not in ROLE_RANK:
        await message.answer("Роли: seeker | moderator | admin | head_admin | creator")
        return

    target_id = message.reply_to_message.from_user.id

    if new_role == ROLE_CREATOR and role_me != ROLE_CREATOR:
        await message.answer("Только Создатель чата (Telegram creator) может назначать роль creator.")
        return

    await set_role_local(message.chat.id, target_id, new_role)

    target = await display_user_mention(bot, message.chat.id, target_id)
    await message.answer(f"✅ Роль назначена: {target} → <b>{h(ROLE_TITLES[new_role])}</b>", parse_mode="HTML")

    who = await display_user_mention(bot, message.chat.id, message.from_user.id)
    await log_action(
        bot,
        message.chat.id,
        f"🧩 <b>SETROLE</b>\nКто: {who}\nКому: {target}\nРоль: <b>{h(ROLE_TITLES[new_role])}</b>\nЧат: <code>{message.chat.id}</code>",
    )


@dp.message(Command("delrole"))
async def cmd_delrole(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP) or not message.from_user:
        return

    role_me = await get_effective_role(bot, message.chat.id, message.from_user.id)
    if not can_use(role_me, "delrole"):
        await message.answer("⛔ Недостаточно прав.")
        return

    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.answer("Использование: ответь на сообщение пользователя и напиши: /delrole")
        return

    target_id = message.reply_to_message.from_user.id
    await del_role_local(message.chat.id, target_id)

    target = await display_user_mention(bot, message.chat.id, target_id)
    await message.answer(f"✅ Роль удалена у {target}.", parse_mode="HTML")

    who = await display_user_mention(bot, message.chat.id, message.from_user.id)
    await log_action(
        bot,
        message.chat.id,
        f"🧩 <b>DELROLE</b>\nКто: {who}\nУ кого: {target}\nЧат: <code>{message.chat.id}</code>",
    )


# =========================
# SETTINGS UI (только HEAD/CREATOR)
# =========================
@dp.message(Command("settings"))
async def cmd_settings(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Команда работает в группах/супергруппах.")
        return
    if not message.from_user:
        return

    role = await get_effective_role(bot, message.chat.id, message.from_user.id)
    if not can_use(role, "settings"):
        await message.answer("⛔ Эта команда доступна только Руководителю Админов/Создателю.")
        return

    await render_settings(bot, message.chat.id, page=PAGE_MAIN, target_message=message)


@dp.message(Command("inactive"))
async def cmd_inactive(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Команда работает в группах/супергруппах.")
        return
    if not message.from_user:
        return

    role = await get_effective_role(bot, message.chat.id, message.from_user.id)
    if not can_use(role, "inactive"):
        await message.answer("⛔ Эта команда доступна только Руководителю Админов/Создателю.")
        return

    sent = await message.answer("📋 Готовлю список неактивных…")
    fake_call = CallbackQuery(
        id="0",
        from_user=message.from_user,
        chat_instance="0",
        message=sent,
        data="tg:inactive_list:0",
    )  # type: ignore
    await render_inactive_list(fake_call, bot, message.chat.id, 0)


# =========================
# WHITELIST COMMANDS (ТОЛЬКО CREATOR/HEAD)
# =========================
def _wl_allowed(role: Optional[str]) -> bool:
    return role in (ROLE_CREATOR, ROLE_HEAD)


@dp.message(Command("wl_add"))
async def cmd_wl_add(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP) or not message.from_user:
        return
    role = await get_effective_role(bot, message.chat.id, message.from_user.id)
    if not _wl_allowed(role):
        await message.answer("⛔ Только Создатель/Руководитель Админов.")
        return

    target_id = await resolve_target_user_id(message)
    if not target_id:
        await message.answer("Использование: reply на пользователя и /wl_add, либо /wl_add @username /wl_add <id>")
        return

    await whitelist_add(message.chat.id, target_id)
    target = await display_user_mention(bot, message.chat.id, target_id)
    await message.answer(f"✅ Добавлен в whitelist: {target}", parse_mode="HTML")


@dp.message(Command("wl_del"))
async def cmd_wl_del(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP) or not message.from_user:
        return
    role = await get_effective_role(bot, message.chat.id, message.from_user.id)
    if not _wl_allowed(role):
        await message.answer("⛔ Только Создатель/Руководитель Админов.")
        return

    target_id = await resolve_target_user_id(message)
    if not target_id:
        await message.answer("Использование: reply на пользователя и /wl_del, либо /wl_del @username /wl_del <id>")
        return

    await whitelist_remove(message.chat.id, target_id)
    target = await display_user_mention(bot, message.chat.id, target_id)
    await message.answer(f"✅ Удалён из whitelist: {target}", parse_mode="HTML")


@dp.message(Command("wl_list"))
async def cmd_wl_list(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP) or not message.from_user:
        return
    role = await get_effective_role(bot, message.chat.id, message.from_user.id)
    if not _wl_allowed(role):
        await message.answer("⛔ Только Создатель/Руководитель Админов.")
        return

    ids = whitelist_list(message.chat.id)
    if not ids:
        await message.answer("Whitelist пуст.")
        return

    lines = ["✅ <b>Whitelist</b>", ""]
    for uid in ids[:200]:
        lines.append(f"• {await display_user_mention(bot, message.chat.id, uid)}")
    if len(ids) > 200:
        lines.append(f"\n…и ещё <b>{len(ids)-200}</b>")

    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.callback_query(F.data.startswith("ui:page:"))
async def cb_ui_page(call: CallbackQuery, bot: Bot):
    if not call.from_user:
        return
    chat_id = call.message.chat.id

    role = await get_effective_role(bot, chat_id, call.from_user.id)
    if not can_use(role, "settings"):
        await call.answer("Только для Руководителя Админов/Создателя.", show_alert=True)
        return

    page = call.data.split(":")[-1]
    await render_settings(bot, chat_id, page=page, edit_cb=call)
    await call.answer()


@dp.callback_query(F.data.startswith("tg:"))
async def cb_settings(call: CallbackQuery, bot: Bot):
    if not call.from_user:
        return

    chat_id = call.message.chat.id
    role = await get_effective_role(bot, chat_id, call.from_user.id)

    # whitelist UI: тоже только creator/head
    if call.data.startswith("tg:wl_"):
        if not _wl_allowed(role):
            await call.answer("Только Создатель/Руководитель.", show_alert=True)
            return

    # settings: только head/creator
    if not can_use(role, "settings"):
        await call.answer("Только для Руководителя Админов/Создателя.", show_alert=True)
        return

    s = get_settings_local(chat_id)
    data = call.data

    try:
        if data.startswith("tg:inactive_list:"):
            page = int(data.split(":")[-1])
            await render_inactive_list(call, bot, chat_id, max(0, page))
            await call.answer()
            return

        if data.startswith("tg:wl_list:"):
            page = int(data.split(":")[-1])
            await render_wl_list(call, bot, chat_id, max(0, page))
            await call.answer()
            return

        if data == "tg:wl_hint_add":
            await call.answer("Добавить: ответь на сообщение пользователя и напиши /wl_add", show_alert=True)
            return

        if data == "tg:wl_hint_del":
            await call.answer("Удалить: ответь на сообщение пользователя и напиши /wl_del", show_alert=True)
            return

        if data == "tg:toggle_enabled":
            await set_setting_local(chat_id, "enabled", not s.enabled)
            await render_settings(bot, chat_id, page=PAGE_MAIN, edit_cb=call)
            await call.answer()
            return

        if data == "tg:toggle_links":
            await set_setting_local(chat_id, "block_links", not s.block_links)
            await render_settings(bot, chat_id, page=PAGE_MAIN, edit_cb=call)
            await call.answer()
            return

        if data == "tg:action_toggle":
            await set_setting_local(chat_id, "action", "mute" if s.action == "delete" else "delete")
            await render_settings(bot, chat_id, page=PAGE_MAIN, edit_cb=call)
            await call.answer()
            return

        if data == "tg:mute_4h":
            await set_setting_local(chat_id, "action", "mute")
            await set_setting_local(chat_id, "mute_seconds", 14400)
            await render_settings(bot, chat_id, page=PAGE_MAIN, edit_cb=call)
            await call.answer()
            return

        if data == "tg:mute_plus30":
            await set_setting_local(chat_id, "mute_seconds", min(86400, s.mute_seconds + 30))
            await render_settings(bot, chat_id, page=PAGE_MAIN, edit_cb=call)
            await call.answer()
            return

        if data == "tg:mute_minus30":
            await set_setting_local(chat_id, "mute_seconds", max(30, s.mute_seconds - 30))
            await render_settings(bot, chat_id, page=PAGE_MAIN, edit_cb=call)
            await call.answer()
            return

        if data == "tg:flood_inc":
            await set_setting_local(chat_id, "flood_limit", min(50, s.flood_limit + 1))
            await render_settings(bot, chat_id, page=PAGE_TEXT, edit_cb=call)
            await call.answer()
            return
        if data == "tg:flood_dec":
            await set_setting_local(chat_id, "flood_limit", max(2, s.flood_limit - 1))
            await render_settings(bot, chat_id, page=PAGE_TEXT, edit_cb=call)
            await call.answer()
            return

        if data == "tg:window_inc":
            await set_setting_local(chat_id, "flood_window_sec", min(120, s.flood_window_sec + 1))
            await render_settings(bot, chat_id, page=PAGE_TEXT, edit_cb=call)
            await call.answer()
            return
        if data == "tg:window_dec":
            await set_setting_local(chat_id, "flood_window_sec", max(3, s.flood_window_sec - 1))
            await render_settings(bot, chat_id, page=PAGE_TEXT, edit_cb=call)
            await call.answer()
            return

        if data == "tg:repeat_inc":
            await set_setting_local(chat_id, "repeat_limit", min(10, s.repeat_limit + 1))
            await render_settings(bot, chat_id, page=PAGE_TEXT, edit_cb=call)
            await call.answer()
            return
        if data == "tg:repeat_dec":
            await set_setting_local(chat_id, "repeat_limit", max(2, s.repeat_limit - 1))
            await render_settings(bot, chat_id, page=PAGE_TEXT, edit_cb=call)
            await call.answer()
            return

        if data == "tg:sticker_mode":
            modes = ["allow", "limit", "ban"]
            nxt = modes[(modes.index(s.sticker_mode) + 1) % len(modes)]
            await set_setting_local(chat_id, "sticker_mode", nxt)
            await render_settings(bot, chat_id, page=PAGE_MEDIA, edit_cb=call)
            await call.answer()
            return

        if data == "tg:gif_mode":
            modes = ["allow", "limit", "ban"]
            nxt = modes[(modes.index(s.gif_mode) + 1) % len(modes)]
            await set_setting_local(chat_id, "gif_mode", nxt)
            await render_settings(bot, chat_id, page=PAGE_MEDIA, edit_cb=call)
            await call.answer()
            return

        if data == "tg:sticker_lim_inc":
            await set_setting_local(chat_id, "sticker_limit", min(30, s.sticker_limit + 1))
            await render_settings(bot, chat_id, page=PAGE_MEDIA, edit_cb=call)
            await call.answer()
            return
        if data == "tg:sticker_lim_dec":
            await set_setting_local(chat_id, "sticker_limit", max(1, s.sticker_limit - 1))
            await render_settings(bot, chat_id, page=PAGE_MEDIA, edit_cb=call)
            await call.answer()
            return

        if data == "tg:gif_lim_inc":
            await set_setting_local(chat_id, "gif_limit", min(30, s.gif_limit + 1))
            await render_settings(bot, chat_id, page=PAGE_MEDIA, edit_cb=call)
            await call.answer()
            return
        if data == "tg:gif_lim_dec":
            await set_setting_local(chat_id, "gif_limit", max(1, s.gif_limit - 1))
            await render_settings(bot, chat_id, page=PAGE_MEDIA, edit_cb=call)
            await call.answer()
            return

        if data == "tg:media_window_inc":
            await set_setting_local(chat_id, "media_window_sec", min(120, s.media_window_sec + 1))
            await render_settings(bot, chat_id, page=PAGE_MEDIA, edit_cb=call)
            await call.answer()
            return
        if data == "tg:media_window_dec":
            await set_setting_local(chat_id, "media_window_sec", max(3, s.media_window_sec - 1))
            await render_settings(bot, chat_id, page=PAGE_MEDIA, edit_cb=call)
            await call.answer()
            return

        if data == "tg:cleanup_toggle":
            await set_setting_local(chat_id, "cleanup_enabled", not s.cleanup_enabled)
            await render_settings(bot, chat_id, page=PAGE_CLEANUP, edit_cb=call)
            await call.answer()
            return

        if data == "tg:cleanup_days_cycle":
            options = [7, 14, 30, 60, 90]
            curd = s.cleanup_days if s.cleanup_days in options else 14
            nxt = options[(options.index(curd) + 1) % len(options)]
            await set_setting_local(chat_id, "cleanup_days", nxt)
            await render_settings(bot, chat_id, page=PAGE_CLEANUP, edit_cb=call)
            await call.answer()
            return

        if data == "tg:cleanup_mode":
            modes = ["kick", "ban"]
            nxt = modes[(modes.index(s.cleanup_mode) + 1) % len(modes)]
            await set_setting_local(chat_id, "cleanup_mode", nxt)
            await render_settings(bot, chat_id, page=PAGE_CLEANUP, edit_cb=call)
            await call.answer()
            return

        if data == "tg:cleanup_run_now":
            processed, removed = await run_cleanup_once(bot, chat_id)
            await render_settings(bot, chat_id, page=PAGE_CLEANUP, edit_cb=call)
            await call.answer(f"Готово: проверено {processed}, удалено {removed}.", show_alert=True)
            return

        await call.answer("Неизвестная команда.", show_alert=True)

    except Exception:
        await call.answer("Ошибка обработки.", show_alert=True)


# =========================
# MODERATION COMMANDS
# =========================
async def parse_reason(parts: List[str], start_index: int) -> str:
    r = " ".join(parts[start_index:]).strip()
    return r if r else "без указания"


@dp.message(Command("to_main"))
@dp.message(F.text.regexp(r"^/(перенести|воснову)(@[\w_]+)?(\s|$)"))
async def cmd_to_main(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP) or not message.from_user:
        return

    if TEST_CHAT_ID is None or MAIN_CHAT_ID is None:
        await message.answer("⚠️ TEST_CHAT_ID / MAIN_CHAT_ID не настроены в .env")
        return

    if message.chat.id != TEST_CHAT_ID:
        await message.answer("⛔ Эта команда работает только в тестовой группе.")
        return

    role = await get_effective_role(bot, message.chat.id, message.from_user.id)
    if not can_use(role, "to_main"):
        await message.answer("⛔ Недостаточно прав для переноса в основную.")
        return

    if not message.reply_to_message:
        await message.answer(
            "Использование:\n"
            "1) Ответь (reply) на сообщение в топике\n"
            "2) Напиши:\n"
            "• /to_main\n"
            "• /перенести\n"
            "• /перенести --del (скопировать и удалить оригинал)"
        )
        return

    src = message.reply_to_message
    delete_original = "--del" in (message.text or "")

    try:
        await bot.copy_message(
            chat_id=MAIN_CHAT_ID,
            from_chat_id=message.chat.id,
            message_id=src.message_id,
        )
    except TelegramBadRequest:
        await message.answer("❌ Не удалось отправить в основную (проверь права бота в основной группе).")
        return

    actor = await display_user_mention(bot, message.chat.id, message.from_user.id)
    topic_id = getattr(src, "message_thread_id", None) or getattr(message, "message_thread_id", None)

    info = (
        "📤 <b>Перенос из тестовой в основную</b>\n"
        f"Кто: {actor}\n"
        f"Тест (chat): <code>{message.chat.id}</code>\n"
        f"Топик (thread): <code>{topic_id if topic_id else '—'}</code>\n"
        f"Message ID: <code>{src.message_id}</code>"
    )

    try:
        await bot.send_message(MAIN_CHAT_ID, info, parse_mode="HTML", disable_web_page_preview=True)
    except TelegramBadRequest:
        pass

    await log_action(
        bot,
        message.chat.id,
        f"📤 <b>MOVE TO MAIN</b>\n"
        f"Кто: {actor}\n"
        f"Откуда: <code>{message.chat.id}</code> (test)\n"
        f"Топик: <code>{topic_id if topic_id else '—'}</code>\n"
        f"Сообщение: <code>{src.message_id}</code>\n"
        f"Куда: <code>{MAIN_CHAT_ID}</code> (main)\n"
        f"Удалить оригинал: <code>{'yes' if delete_original else 'no'}</code>",
    )

    if delete_original:
        try:
            await src.delete()
        except TelegramBadRequest:
            await message.answer("⚠️ Скопировал, но не смог удалить оригинал (нет прав на удаление).")
            return

    await message.answer("✅ Готово. Сообщение отправлено в основную.")


@dp.message(Command("mute"))
async def cmd_mute(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP) or not message.from_user:
        return

    role = await get_effective_role(bot, message.chat.id, message.from_user.id)
    if not can_use(role, "mute"):
        await message.answer("⛔ Недостаточно прав для /mute.")
        return

    parts = split_command_args(message.text or "")
    target_id = await resolve_target_user_id(message)
    if not target_id:
        await message.answer(
            "Использование:\n"
            "• reply: /mute 10m причина\n"
            "• /mute @username 10m причина\n"
            "• /mute <user_id> 10m причина\n\n"
            "Пример:\n"
            "<code>/mute \"@UserName\" \"10m\" \"спам\"</code>",
            parse_mode="HTML",
        )
        return

    if not await ensure_can_moderate_target(bot, message.chat.id, message.from_user.id, target_id):
        await message.answer("⛔ Нельзя применить действие к этому пользователю.")
        return

    if message.reply_to_message:
        if len(parts) < 2:
            await message.answer("Укажи время: /mute 10m причина")
            return
        dur = parse_duration_to_seconds(parts[1])
        reason = await parse_reason(parts, 2)
    else:
        if len(parts) < 3:
            await message.answer("Укажи время: /mute @username 10m причина")
            return
        dur = parse_duration_to_seconds(parts[2])
        reason = await parse_reason(parts, 3)

    if not dur or dur < 30:
        await message.answer("Некорректное время. Пример: 10m, 2h, 1d, 2ч30м")
        return

    until = int(time.time()) + dur
    perms = ChatPermissions(can_send_messages=False)

    try:
        await bot.restrict_chat_member(message.chat.id, target_id, permissions=perms, until_date=until)
    except TelegramBadRequest:
        await message.answer("Не удалось замутить (нет прав у бота или пользователь админ).")
        return

    actor = await display_user_mention(bot, message.chat.id, message.from_user.id)
    target = await display_user_mention(bot, message.chat.id, target_id)

    await message.answer(
        f"✅ Пользователь {target} был <b>замучен</b> на <b>{h(format_duration(dur))}</b>.\n"
        f"Причина: <code>{h(reason)}</code>",
        parse_mode="HTML",
    )

    await log_action(
        bot,
        message.chat.id,
        f"🔇 <b>MUTE</b>\nЧат: <code>{message.chat.id}</code>\n"
        f"Кто: {actor}\nКого: {target}\n"
        f"Срок: <code>{dur}</code> сек\n"
        f"Причина: <code>{h(reason)}</code>",
    )


@dp.message(Command("unmute"))
async def cmd_unmute(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP) or not message.from_user:
        return

    role = await get_effective_role(bot, message.chat.id, message.from_user.id)
    if not can_use(role, "unmute"):
        await message.answer("⛔ Недостаточно прав для /unmute.")
        return

    target_id = await resolve_target_user_id(message)
    if not target_id:
        await message.answer("Использование:\n• reply: /unmute\n• /unmute @username\n• /unmute <user_id>")
        return

    if not await ensure_can_moderate_target(bot, message.chat.id, message.from_user.id, target_id):
        await message.answer("⛔ Нельзя применить действие к этому пользователю.")
        return

    perms = ChatPermissions(
        can_send_messages=True,
        can_send_other_messages=True,
        can_send_audios=True,
        can_send_documents=True,
        can_send_photos=True,
        can_send_videos=True,
        can_send_video_notes=True,
        can_send_voice_notes=True,
        can_send_polls=True,
        can_add_web_page_previews=True,
    )

    try:
        await bot.restrict_chat_member(message.chat.id, target_id, permissions=perms, until_date=0)
    except TelegramBadRequest:
        await message.answer("Не удалось размутить (нет прав у бота или пользователь админ).")
        return

    actor = await display_user_mention(bot, message.chat.id, message.from_user.id)
    target = await display_user_mention(bot, message.chat.id, target_id)

    await message.answer(f"✅ Пользователь {target} был <b>размучен</b>.", parse_mode="HTML")
    await log_action(
        bot,
        message.chat.id,
        f"🔊 <b>UNMUTE</b>\nЧат: <code>{message.chat.id}</code>\nКто: {actor}\nКого: {target}",
    )


@dp.message(Command("ban"))
async def cmd_ban(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP) or not message.from_user:
        return

    role = await get_effective_role(bot, message.chat.id, message.from_user.id)
    if not can_use(role, "ban"):
        await message.answer("⛔ Недостаточно прав для /ban.")
        return

    parts = split_command_args(message.text or "")
    target_id = await resolve_target_user_id(message)
    if not target_id:
        await message.answer(
            "Использование:\n"
            "• reply: /ban 7d причина (время опционально)\n"
            "• /ban @username 7d причина\n"
            "• /ban <user_id> 7d причина\n\n"
            "Пример:\n<code>/ban \"@UserName\" \"7d\" \"реклама\"</code>",
            parse_mode="HTML",
        )
        return

    if not await ensure_can_moderate_target(bot, message.chat.id, message.from_user.id, target_id):
        await message.answer("⛔ Нельзя применить действие к этому пользователю.")
        return

    dur: Optional[int] = None
    reason = "без указания"

    if message.reply_to_message:
        if len(parts) >= 2:
            maybe = parse_duration_to_seconds(parts[1])
            if maybe:
                dur = maybe
                reason = await parse_reason(parts, 2)
            else:
                reason = await parse_reason(parts, 1)
    else:
        if len(parts) >= 3:
            maybe = parse_duration_to_seconds(parts[2])
            if maybe:
                dur = maybe
                reason = await parse_reason(parts, 3)
            else:
                reason = await parse_reason(parts, 2)

    until = 0
    dur_txt = "навсегда"
    if dur:
        until = int(time.time()) + dur
        dur_txt = format_duration(dur)

    try:
        await bot.ban_chat_member(message.chat.id, target_id, until_date=until)
    except TelegramBadRequest:
        await message.answer("Не удалось забанить (нет прав у бота или пользователь админ).")
        return

    actor = await display_user_mention(bot, message.chat.id, message.from_user.id)
    target = await display_user_mention(bot, message.chat.id, target_id)

    await message.answer(
        f"⛔ Пользователь {target} был <b>заблокирован</b> ({h(dur_txt)}).\n"
        f"Причина: <code>{h(reason)}</code>",
        parse_mode="HTML",
    )

    await log_action(
        bot,
        message.chat.id,
        f"⛔ <b>BAN</b>\nЧат: <code>{message.chat.id}</code>\n"
        f"Кто: {actor}\nКого: {target}\n"
        f"Срок: <code>{h(dur_txt)}</code>\n"
        f"Причина: <code>{h(reason)}</code>",
    )


@dp.message(Command("unban"))
async def cmd_unban(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP) or not message.from_user:
        return

    role = await get_effective_role(bot, message.chat.id, message.from_user.id)
    if not can_use(role, "unban"):
        await message.answer("⛔ Недостаточно прав для /unban.")
        return

    target_id = await resolve_target_user_id(message)
    if not target_id:
        await message.answer("Использование:\n• reply: /unban\n• /unban @username\n• /unban <user_id>")
        return

    if not await ensure_can_moderate_target(bot, message.chat.id, message.from_user.id, target_id):
        await message.answer("⛔ Нельзя применить действие к этому пользователю.")
        return

    try:
        await bot.unban_chat_member(message.chat.id, target_id)
    except TelegramBadRequest:
        await message.answer("Не удалось разбанить (нет прав у бота).")
        return

    actor = await display_user_mention(bot, message.chat.id, message.from_user.id)
    target = await display_user_mention(bot, message.chat.id, target_id)

    await message.answer(f"✅ Пользователь {target} был <b>разблокирован</b>.", parse_mode="HTML")
    await log_action(
        bot,
        message.chat.id,
        f"✅ <b>UNBAN</b>\nЧат: <code>{message.chat.id}</code>\nКто: {actor}\nКого: {target}",
    )


@dp.message(Command("kick"))
async def cmd_kick(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP) or not message.from_user:
        return

    role = await get_effective_role(bot, message.chat.id, message.from_user.id)
    if not can_use(role, "kick"):
        await message.answer("⛔ Недостаточно прав для /kick.")
        return

    target_id = await resolve_target_user_id(message)
    if not target_id:
        await message.answer("Использование:\n• reply: /kick причина\n• /kick @username причина\n• /kick <user_id> причина")
        return

    if not await ensure_can_moderate_target(bot, message.chat.id, message.from_user.id, target_id):
        await message.answer("⛔ Нельзя применить действие к этому пользователю.")
        return

    parts = split_command_args(message.text or "")
    if message.reply_to_message:
        reason = await parse_reason(parts, 1)
    else:
        reason = await parse_reason(parts, 2)

    try:
        await bot.ban_chat_member(message.chat.id, target_id)
        await bot.unban_chat_member(message.chat.id, target_id)
    except TelegramBadRequest:
        await message.answer("Не удалось кикнуть (нет прав у бота или пользователь админ).")
        return

    actor = await display_user_mention(bot, message.chat.id, message.from_user.id)
    target = await display_user_mention(bot, message.chat.id, target_id)

    await message.answer(
        f"👢 Пользователь {target} был <b>исключён</b>.\nПричина: <code>{h(reason)}</code>",
        parse_mode="HTML",
    )

    await log_action(
        bot,
        message.chat.id,
        f"👢 <b>KICK</b>\nЧат: <code>{message.chat.id}</code>\n"
        f"Кто: {actor}\nКого: {target}\n"
        f"Причина: <code>{h(reason)}</code>",
    )


@dp.message(Command("warn"))
async def cmd_warn(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP) or not message.from_user:
        return

    role = await get_effective_role(bot, message.chat.id, message.from_user.id)
    if not can_use(role, "warn"):
        await message.answer("⛔ Недостаточно прав для /warn.")
        return

    parts = split_command_args(message.text or "")
    target_id = await resolve_target_user_id(message)
    if not target_id:
        await message.answer(
            "Использование:\n"
            "• reply: /warn причина\n"
            "• /warn @username причина\n"
            "• /warn <user_id> причина\n\n"
            "Пример:\n<code>/warn \"@UserName\" \"оскорбления\"</code>",
            parse_mode="HTML",
        )
        return

    if not await ensure_can_moderate_target(bot, message.chat.id, message.from_user.id, target_id):
        await message.answer("⛔ Нельзя применить действие к этому пользователю.")
        return

    if message.reply_to_message:
        reason = await parse_reason(parts, 1)
    else:
        reason = await parse_reason(parts, 2)

    cnt = await add_warn_local(message.chat.id, target_id, message.from_user.id, reason)

    actor = await display_user_mention(bot, message.chat.id, message.from_user.id)
    target = await display_user_mention(bot, message.chat.id, target_id)

    await message.answer(
        f"⚠️ Пользователь {target} получил <b>WARN</b> (#{cnt}).\nПричина: <code>{h(reason)}</code>",
        parse_mode="HTML",
    )

    await log_action(
        bot,
        message.chat.id,
        f"⚠️ <b>WARN</b>\nЧат: <code>{message.chat.id}</code>\n"
        f"Кто: {actor}\nКого: {target}\n"
        f"Номер: <code>#{cnt}</code>\n"
        f"Причина: <code>{h(reason)}</code>",
    )

    # Авто-наказание: после каждого 3-го warn — мут на 1 час
    if cnt % 3 == 0:
        auto_mute_sec = 3600
        until = int(time.time()) + auto_mute_sec
        perms = ChatPermissions(can_send_messages=False)
        try:
            await bot.restrict_chat_member(message.chat.id, target_id, permissions=perms, until_date=until)

            await message.answer(
                "🔇 <b>Авто-наказание</b>\n"
                f"Пользователь: {target}\n"
                f"Warn: <b>{cnt}</b>\n"
                f"Мут: <b>1 час</b>\n"
                f"Основание: <b>3 предупреждения</b>\n"
                f"Последняя причина warn: <code>{h(reason)}</code>",
                parse_mode="HTML",
            )

            await log_action(
                bot,
                message.chat.id,
                "🔇 <b>AUTO-MUTE BY WARNS</b>\n"
                f"Чат: <code>{message.chat.id}</code>\n"
                f"Кто выдал warn: {actor}\n"
                f"Кого: {target}\n"
                f"Warn count: <code>{cnt}</code>\n"
                f"Срок: <code>3600</code> сек\n"
                f"Основание: <code>3 предупреждения</code>\n"
                f"Последняя причина warn: <code>{h(reason)}</code>",
            )
        except TelegramBadRequest:
            pass


# =========================
# MODERATION CORE (automod)
# =========================
@dp.message()
async def moderate_all(message: Message, bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    if not message.from_user or message.from_user.is_bot:
        return

    await upsert_activity_local(message.chat.id, message.from_user.id, int(time.time()), message.from_user.username)

    s = get_settings_local(message.chat.id)
    if not s.enabled:
        return

    uid = message.from_user.id
    cid = message.chat.id
    now = time.time()

    # whitelist: полностью не трогаем
    if is_whitelisted(cid, uid):
        return

    # модераторов и выше не трогаем
    role = await get_effective_role(bot, cid, uid)
    if role and role_at_least(role, ROLE_MOD):
        return

    try:
        member = await bot.get_chat_member(cid, uid)
        if member.status in ("administrator", "creator"):
            return
    except TelegramBadRequest:
        pass

    # --- FIX: альбомы (media_group) считаем как 1 событие, чтобы не мутить за пачку фото ---
    mgid = getattr(message, "media_group_id", None)
    if mgid:
        seen = album_seen[cid][uid]
        key = str(mgid)

        # все элементы кроме первого — игнорируем
        if key in seen:
            return

        # первый элемент: отметили и проверили caption на ссылку
        seen[key] = now
        if len(seen) > 300:
            for k, ts in sorted(seen.items(), key=lambda x: x[1])[:100]:
                seen.pop(k, None)

        cap = norm_text(message.caption or "")
        if cap and s.block_links and contains_link(cap):
            await apply_action(bot, message, s, "album_link")
        return

    # stickers
    if message.sticker:
        mode = s.sticker_mode
        if mode == "ban":
            await apply_action(bot, message, s, "sticker_ban")
            return
        if mode == "limit":
            dq = sticker_times[cid][uid]
            dq.append(now)
            while dq and (now - dq[0]) > s.media_window_sec:
                dq.popleft()
            if len(dq) > s.sticker_limit:
                await apply_action(bot, message, s, "sticker_limit")
                return
        return

    # animations/gif/video-as-animation
    if message.animation:
        mode = s.gif_mode
        if mode == "ban":
            await apply_action(bot, message, s, "gif_ban")
            return
        if mode == "limit":
            dq = gif_times[cid][uid]
            dq.append(now)
            while dq and (now - dq[0]) > s.media_window_sec:
                dq.popleft()
            if len(dq) > s.gif_limit:
                await apply_action(bot, message, s, "gif_limit")
                return
        return

    # text / caption
    text = message.text or message.caption or ""
    tnorm = norm_text(text)

    # flood
    dq = msg_times[cid][uid]
    dq.append(now)
    while dq and (now - dq[0]) > s.flood_window_sec:
        dq.popleft()
    if len(dq) > s.flood_limit:
        await apply_action(bot, message, s, "flood")
        return

    # links
    if s.block_links and tnorm and contains_link(tnorm):
        await apply_action(bot, message, s, "link")
        return

    # repeat
    if tnorm:
        hsh_ = text_hash(tnorm)
        last_h, count = last_hash[cid][uid]
        count = (count + 1) if (hsh_ == last_h) else 1
        last_hash[cid][uid] = (hsh_, count)
        if count >= s.repeat_limit:
            await apply_action(bot, message, s, "repeat")
            return


# =========================
# MAIN
# =========================
async def main():
    await load_data()

    bot = Bot(BOT_TOKEN)
    try:
        chat_ids = [cid for cid in [TEST_CHAT_ID, MAIN_CHAT_ID] if isinstance(cid, int)]
        if chat_ids:
            await setup_bot_commands(bot, chat_ids)

        asyncio.create_task(cleanup_loop(bot))
        asyncio.create_task(prune_activity_loop())

        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
