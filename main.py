import os
import sys
import asyncio
import signal
import time
import platform
from datetime import datetime, timedelta
from typing import Set, Dict, Optional
import aiosqlite
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    filters,
    ContextTypes,
    ApplicationBuilder,
)
from telegram.error import TelegramError, NetworkError, TimedOut, BadRequest
from telegram.request import HTTPXRequest
import pytz
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from functools import wraps
from loguru import logger
import subprocess
import re

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logger.remove()
logger.add("bot.log", rotation="1 MB", retention=10, level="INFO", encoding="utf-8", backtrace=True, diagnose=True, compression="zip")
logger.add(sys.stdout, level="INFO", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")

# Константы
MAX_ATTEMPTS = 3
ENTER_SECRET_CODE = 1
DB_TIMEOUT = 10
RESTART_DELAY = 60
CONTROL_RESTART_INTERVAL = 1200  # 20 минут
HEARTBEAT_INTERVAL = 300  # 5 минут
MAX_VIOLATIONS = 3
MIN_MESSAGE_LENGTH = 10
MAX_RESTART_ATTEMPTS = 5
SYNC_INTERVAL = 30 * 24 * 3600  # 30 дней
CLEAN_VIOLATIONS_INTERVAL = 50 * 24 * 3600
REQUEST_TIMEOUT = 120

# Лимиты для rate limiting
RATE_LIMITS = {
    "default": 5,
    "start": 10,
    "rules": 5,
    "help": 5,
    "stats": 30,
    "restart": 60,
    "status": 30,
    "contacts": 5,
}

# Переменные окружения
try:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_IDS = set(map(int, filter(None, os.getenv("ADMIN_IDS", "").split(","))))
    SECRET_CODE = os.getenv("SECRET_CODE")
    GROUP_ID = int(os.getenv("GROUP_ID"))
    CHANNEL_URL = os.getenv("CHANNEL_URL")
    TIMEZONE = pytz.timezone("Asia/Vladivostok")  # Хабаровск, UTC+10
    WELCOME_MESSAGE_TIMEOUT = int(os.getenv("WELCOME_MESSAGE_TIMEOUT", 300))
    VIOLATION_TIMEOUT_HOURS = int(os.getenv("VIOLATION_TIMEOUT_HOURS", 24))
    NIGHT_START = 23  # 23:00
    NIGHT_END = 7     # 07:00
    OWNER_ID = int(os.getenv("OWNER_ID"))
except (ValueError, TypeError) as e:
    logger.critical(f"Ошибка в переменных окружения: {e}")
    sys.exit(1)

if not all([BOT_TOKEN, SECRET_CODE, CHANNEL_URL]):
    logger.critical("Отсутствуют обязательные переменные окружения!")
    sys.exit(1)

# Сообщения
WELCOME_TEXT = (
    "🌄✨ **Привет, {name}!** 🌟\n"
    "🏕️🌲 Добро пожаловать в **«Палатки-ДВ»** — место, где начинаются твои лучшие приключения!\n\n"
    "👉 **Что у нас интересного:**\n"
    "✅ **Палатки** — летние, зимние, семейные;\n"
    "✅ **Фонари** — мощные и пылевлагозащищённые;\n"
    "✅ **Обогреватели** — керосиновые, дровяные печки;\n"
    "✅ **Снаряжение** — спальники, коврики, термосы, спецодежда, рюкзаки;\n"
    "✅ **Кемпинговая мебель** — стулья, столы, шезлонги, тенты;\n"
    "✅ **Рыбацкие принадлежности** — всё для комфортной рыбалки;\n"
    "✅ **И многое другое** — солнечные панели, наборы для выживания и множество других нужных принадлежностей!\n\n"
    "📜 **Ознакомься с <a href='/rules'>правилами</a> и вливайся в наше дружное сообщество!**\n\n"
    "💬 **Если вдруг не найдёшь чего-то, всегда спрашивай, пиши или звони — мы всегда с радостью поможем и проконсультируем!**"
)

RULES_TEXT = (
    "📜 <b>Правила сообщества \"Палатки-ДВ\"</b>\n\n"
    "🚫 <b>Запрещено:</b>\n"
    "1. Мат, оскорбления, агрессия;\n"
    "2. Реклама и спам без согласования;\n"
    "3. Флуд, оффтоп, повторяющиеся сообщения;\n"
    "4. Публикация личных данных без согласия;\n"
    "5. Разжигание конфликтов, провокации;\n"
    "6. Публикация ссылок без согласования с администрацией;\n"
    "7. Любые действия, нарушающие законодательство РФ.\n\n"
    "✅ <b>Разрешено:</b>\n"
    "1. Обсуждение снаряжения, походов, кемпинга;\n"
    "2. Полезные советы, лайфхаки, рекомендации — <b>сбрасывай админу для размещения</b>;\n"
    "3. Фото и видео из походов и путешествий — <b>сбрасывай админу для размещения</b>;\n"
    "4. Делиться опытом и вдохновением — <b>сбрасывай админу для размещения</b>.\n\n"
    "❗ <b>Важно:</b>\n"
    "- Нарушение правил ведёт к предупреждениям, удалению сообщений или блокировке.\n\n"
    "<b>Соблюдай правила, чтобы сохранить дружелюбную и полезную атмосферу! 🌿</b>"
)

HELP_TEXT = (
    "🌟 <b>Команды для пользователей:</b>\n\n"
    "• <b>/rules</b> — правила сообщества\n\n"
    "• <b>/help</b> — список команд\n\n"
    "• <b>/contacts</b> — контакты для связи\n\n"
    "\n"
    "🌟 <b>Команды для администраторов:</b>\n\n"
    "• <b>/start</b> — активация бота\n\n"
    "• <b>/stats</b> — статистика\n\n"
    "• <b>/status</b> — состояние бота\n\n"
    "\n"
    "• <b>/restart</b> — перезапуск бота\n"
)

CONTACTS_TEXT = (
    "🌟 <b>Контакты «Палатки-ДВ»:</b>\n\n"
    "💬 <b>Telegram:</b> <a href='https://t.me/palatki_lodki_khv'>@palatki_lodki_khv</a>\n\n"
    "📱 <b>WhatsApp:</b> <a href='https://wa.me/+79249203356'>Позвонить / Написать</a>\n\n"
    "🌐 <b>Канал:</b> <a href='{CHANNEL_URL}'>Палатки-ДВ</a>\n\n"
    "Обращайтесь за консультацией или заказом!"
)

BAD_WORDS_PATTERN = re.compile(
    r"(?<!\w)"
    r"(?:б[лb][яa][тtь]?|с[уy][кk][аaи]?|п[иi][з3][дd][еe][цcт]?|х[уy][йiй]|[еeё][бb][аa][тtь]?|"
    r"п[иi][дd][оo][рp]|[мm][уy][дd][аa][кk]|[дd][оo][лl][бb][оoё][бb]|[хx][уy][ёеe][вv][оo]|[пp][иi][з3][дd][аa]|"
    r"[жj][оo][пp][аa]|[нn][аa][хx][уy][йi]|[гg][оo][вv][нn][оo]|[ш][лl][юy][хx][аa]|[хx][уy][еe][сc][оo][сc]|"
    r"[дd][еe][бb][иi][лl]|[иi][дd][иi][оo][тt]|[кk][оo][з3][ёеe][лl]|[лl][оo][хx]|[мm][рp][аa][з3][ьъ]?|[тt][вv][аa][рp][ьъ]?)"
    r"(?![a-zA-Z0-9])",
    re.IGNORECASE | re.UNICODE
)

# Очередь задач
task_queue = asyncio.Queue(maxsize=50)

# База данных и кэш
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
async def init_db() -> None:
    async with aiosqlite.connect("violations.db", timeout=DB_TIMEOUT) as conn:
        await conn.execute(
            '''CREATE TABLE IF NOT EXISTS violations 
               (user_id INTEGER PRIMARY KEY, count INTEGER, last_violation TEXT)'''
        )
        await conn.execute(
            '''CREATE TABLE IF NOT EXISTS subscriptions 
               (user_id INTEGER PRIMARY KEY, subscription_time TEXT)'''
        )
        await conn.commit()

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
async def load_violations_cache(context: ContextTypes.DEFAULT_TYPE) -> None:
    async with aiosqlite.connect("violations.db", timeout=DB_TIMEOUT) as conn:
        async with conn.execute("SELECT user_id, count, last_violation FROM violations") as cursor:
            async for user_id, count, last_violation in cursor:
                context.bot_data.setdefault('violations_cache', {})[user_id] = {
                    "count": count,
                    "last_violation": datetime.fromisoformat(last_violation) if last_violation else None
                }
        async with conn.execute("SELECT user_id, subscription_time FROM subscriptions") as cursor:
            async for user_id, subscription_time in cursor:
                context.bot_data.setdefault('subscriptions_cache', {})[user_id] = {
                    "subscription_time": datetime.fromisoformat(subscription_time)
                }

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
async def sync_violations_cache(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        async with aiosqlite.connect("violations.db", timeout=DB_TIMEOUT) as conn:
            async with conn.cursor() as cursor:
                for user_id, data in context.bot_data.get('violations_cache', {}).items():
                    await cursor.execute(
                        "INSERT OR REPLACE INTO violations (user_id, count, last_violation) VALUES (?, ?, ?)",
                        (user_id, data["count"], data["last_violation"].isoformat() if data["last_violation"] else None)
                    )
                for user_id, data in context.bot_data.get('subscriptions_cache', {}).items():
                    await cursor.execute(
                        "INSERT OR REPLACE INTO subscriptions (user_id, subscription_time) VALUES (?, ?, ?)",
                        (user_id, data["subscription_time"].isoformat())
                    )
                await conn.commit()
    except Exception as e:
        logger.error(f"Ошибка синхронизации кэша: {e}")

async def clean_violations_cache(context: ContextTypes.DEFAULT_TYPE) -> None:
    now = get_current_time()
    cache = context.bot_data.get('violations_cache', {})
    removed = 0
    for user_id in list(cache.keys()):
        if now - cache[user_id]["last_violation"] > timedelta(hours=VIOLATION_TIMEOUT_HOURS):
            del cache[user_id]
            removed += 1
    if removed > 0:
        logger.info(f"Кэш нарушений очищен, удалено {removed} записей")

async def get_violations(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> Dict[str, any]:
    return context.bot_data.get('violations_cache', {}).get(user_id, {"count": 0, "last_violation": None})

async def update_violations(user_id: int, count: int, last_violation: datetime, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.bot_data.setdefault('violations_cache', {})[user_id] = {
        "count": count,
        "last_violation": last_violation
    }

async def update_subscription(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    now = get_current_time()
    context.bot_data.setdefault('subscriptions_cache', {})[user_id] = {
        "subscription_time": now
    }

# Вспомогательные функции
def get_current_time() -> datetime:
    return datetime.now(TIMEZONE)

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def is_night_time() -> bool:
    current_hour = get_current_time().hour
    return NIGHT_START <= current_hour or current_hour < NIGHT_END

def rate_limit(command_name: str = "default"):
    def decorator(func):
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            user_id = update.effective_user.id
            current_time = time.monotonic()
            limit = RATE_LIMITS.get(command_name, RATE_LIMITS["default"])
            last_command = context.bot_data.get(f"last_command_{user_id}_{command_name}", 0)
            if current_time - last_command < limit:
                await update.message.reply_text("⏳ Слишком много запросов. Подожди.")
                return
            context.bot_data[f"last_command_{user_id}_{command_name}"] = current_time
            return await func(update, context, *args, **kwargs)
        return wrapper
    return decorator

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def notify_admins(context: ContextTypes.DEFAULT_TYPE, message: str) -> None:
    for admin_id in ADMIN_IDS:
        await context.bot.send_message(chat_id=admin_id, text=message, parse_mode="HTML")

def create_subscribe_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("👉 ПОДПИСАТЬСЯ НА КАНАЛ 👈", url=CHANNEL_URL)]])

async def get_bot_rights(context: ContextTypes.DEFAULT_TYPE) -> dict:
    if 'bot_rights' not in context.bot_data:
        context.bot_data['bot_rights'] = await context.bot.get_chat_member(chat_id=GROUP_ID, user_id=context.bot.id)
    return context.bot_data['bot_rights']

# Асинхронный воркер
async def task_worker(context: ContextTypes.DEFAULT_TYPE) -> None:
    while True:
        try:
            task = await asyncio.wait_for(task_queue.get(), timeout=30)
            await task()
            task_queue.task_done()
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            logger.error(f"Ошибка в задаче: {e}")
            task_queue.task_done()

def check_duplicate_process() -> bool:
    current_pid = os.getpid()
    script_name = os.path.basename(__file__)
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            cmdline = proc.info['cmdline']
            if proc.pid != current_pid and cmdline and script_name in ' '.join(cmdline):
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, TypeError):
            continue
    return False

# Перезапуск
async def restart_self(context: Optional[ContextTypes.DEFAULT_TYPE] = None) -> None:
    restart_attempts = context.bot_data.get('restart_attempts', 0) if context else 0
    restart_attempts += 1

    if restart_attempts > MAX_RESTART_ATTEMPTS:
        logger.critical(f"Превышено максимальное количество перезапусков ({MAX_RESTART_ATTEMPTS})")
        if context:
            await notify_admins(context, f"🚨 Превышено максимальное количество перезапусков ({MAX_RESTART_ATTEMPTS})")
        await asyncio.sleep(CONTROL_RESTART_INTERVAL)
        restart_attempts = 0

    if context:
        context.bot_data['restart_attempts'] = restart_attempts

    if check_duplicate_process():
        sys.exit(0)

    try:
        logger.info(f"Перезапуск бота (попытка {restart_attempts}/{MAX_RESTART_ATTEMPTS})")
        cmd = [sys.executable, os.path.abspath(__file__)]
        subprocess.Popen(cmd, env=os.environ.copy(), shell=(platform.system() == "Windows"))
        await asyncio.sleep(2)
        sys.exit(0)
    except Exception as e:
        logger.error(f"Ошибка при перезапуске: {e}")
        await asyncio.sleep(RESTART_DELAY * (2 ** min(restart_attempts, 5)))
        await restart_self(context)

# Heartbeat
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
async def heartbeat(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        await context.bot.get_me()
    except Exception as e:
        logger.error(f"Ошибка heartbeat: {e}")
        await restart_self(context)
        raise

# Обработчики
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
async def welcome_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat_id != GROUP_ID or not update.message.new_chat_members:
        return
    for member in update.message.new_chat_members:
        if member.id == context.bot.id:
            continue
        name = member.first_name or "Пользователь"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("👉 ПОДПИСАТЬСЯ", url=CHANNEL_URL)],
            [InlineKeyboardButton("✅ Прочитано", callback_data="welcome_read")]
        ])
        group_msg = await context.bot.send_message(
            chat_id=GROUP_ID,
            text=WELCOME_TEXT.format(name=name),
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=keyboard
        )
        context.job_queue.run_once(
            lambda ctx, msg_id=group_msg.message_id: ctx.bot.delete_message(chat_id=GROUP_ID, message_id=msg_id),
            WELCOME_MESSAGE_TIMEOUT,
            name=f"delete_welcome_{group_msg.message_id}"
        )

async def welcome_read_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    await query.message.delete()
    user_id = query.from_user.id
    await update_subscription(user_id, context)

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
async def night_auto_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat_id != GROUP_ID or not update.message.text or not is_night_time():
        return
    now = get_current_time()
    user_name = update.message.from_user.first_name
    user_id = update.message.from_user.id
    text = update.message.text[:4096] if len(update.message.text) <= 4096 else "Сообщение слишком длинное"
    response = (
        f"🌟 Здравствуйте, {user_name}! 🌟 Это ночной автоответчик 🌙✨\n"
        f"🌙 Наша команда — <b>Палатки-ДВ</b> уже отдыхает, так как у нас ночь ({now.strftime('%H:%M')}). 🛌💤\n"
        "🌄 С первыми утренними лучами мы обязательно вам ответим! 🌅✨\n"
        "🙏 Спасибо за ваше терпение и понимание! 💫"
    )
    keyboard = create_subscribe_keyboard()
    await update.message.reply_text(response, parse_mode="HTML", reply_markup=keyboard)
    await context.bot.send_message(chat_id=OWNER_ID, text=f"🔔 Ночное сообщение от {user_name} (ID: {user_id}): {text}", parse_mode="HTML")

async def check_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat_id != GROUP_ID or not update.message.text or is_admin(update.effective_user.id):
        return
    text = update.message.text.lower()
    if len(text) < MIN_MESSAGE_LENGTH:
        return

    current_date = get_current_time().date()
    if current_date != context.bot_data.get('last_day_reset'):
        context.bot_data['messages_today'] = 0
        context.bot_data['last_day_reset'] = current_date
    context.bot_data['messages_processed'] = context.bot_data.get('messages_processed', 0) + 1
    context.bot_data['messages_today'] = context.bot_data.get('messages_today', 0) + 1

    matches = list(BAD_WORDS_PATTERN.finditer(text))
    if not matches:
        return

    for match in matches:
        word = match.group(0)
        if len(word) < 3 or any(c.isdigit() for c in word) or word in ["бла", "суп", "пика"]:
            continue

        user_id = update.effective_user.id
        now = get_current_time()
        violation_data = await get_violations(user_id, context)
        count = 0 if not violation_data["last_violation"] or (now - violation_data["last_violation"]) > timedelta(hours=VIOLATION_TIMEOUT_HOURS) else violation_data["count"]
        count += 1
        await update_violations(user_id, count, now, context)

        bot_rights = await get_bot_rights(context)
        if bot_rights.can_delete_messages:
            await update.message.delete()
        remaining_lives = MAX_VIOLATIONS - count
        keyboard = create_subscribe_keyboard()
        await context.bot.send_message(
            chat_id=GROUP_ID,
            text=f"⚠️ Нарушение правил! Слово: '{word}'. Осталось предупреждений: {remaining_lives}",
            parse_mode="HTML",
            reply_markup=keyboard
        )
        if count >= MAX_VIOLATIONS and bot_rights.can_restrict_members:
            await context.bot.ban_chat_member(GROUP_ID, user_id)
            context.bot_data.setdefault('banned_users', set()).add(user_id)
            await context.bot.send_message(
                chat_id=GROUP_ID,
                text="🚫 Пользователь заблокирован.",
                reply_markup=keyboard
            )
        break

@rate_limit("rules")
async def show_rules(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = create_subscribe_keyboard()
    await update.message.reply_text(RULES_TEXT, parse_mode="HTML", reply_markup=keyboard)

@rate_limit("help")
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = create_subscribe_keyboard()
    await update.message.reply_text(HELP_TEXT, parse_mode="HTML", reply_markup=keyboard)

@rate_limit("contacts")
async def contacts_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = create_subscribe_keyboard()
    await update.message.reply_text(
        CONTACTS_TEXT.format(CHANNEL_URL=CHANNEL_URL),
        parse_mode="HTML",
        reply_markup=keyboard
    )

@rate_limit("stats")
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("🚫 Команда только для админов!")
        return
    now = get_current_time()
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    subscriptions = context.bot_data.get('subscriptions_cache', {})
    subs_today = sum(1 for data in subscriptions.values() if data["subscription_time"] >= day_start)
    subs_month = sum(1 for data in subscriptions.values() if data["subscription_time"] >= month_start)
    violations = context.bot_data.get('violations_cache', {})
    total_violations = sum(data["count"] for data in violations.values())
    banned_users = len(context.bot_data.get('banned_users', set()))

    message = (
        "📊 <b>Статистика:</b>\n"
        f"👥 Подписавшихся сегодня: {subs_today}\n"
        f"👥 Подписавшихся за месяц: {subs_month}\n"
        f"⚠️ Всего нарушений: {total_violations}\n"
        f"🚫 Заблокированных: {banned_users}"
    )
    await update.message.reply_text(message, parse_mode="HTML")

@rate_limit("restart")
async def restart_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("🚫 Команда только для админов!")
        return
    await update.message.reply_text("🔄 Перезапуск бота...")
    context.bot_data['restart_attempts'] = 0
    await restart_self(context)

@rate_limit("status")
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("🚫 Команда только для админов!")
        return
    uptime = time.time() - context.bot_data.get('start_time', time.time())
    messages_processed = context.bot_data.get('messages_processed', 0)
    messages_today = context.bot_data.get('messages_today', 0)
    restarts = context.bot_data.get('restart_attempts', 0)
    status_text = (
        f"📈 <b>Состояние бота:</b>\n"
        f"⏳ Время работы: {int(uptime // 3600)}ч {int((uptime % 3600) // 60)}м\n"
        f"📩 Обработано сообщений: {messages_processed} (сегодня: {messages_today})\n"
        f"🔄 Перезапусков: {restarts}"
    )
    await update.message.reply_text(status_text, parse_mode="HTML")

@rate_limit("start")
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    activated_users = context.bot_data.setdefault('activated_users', set())
    if user_id in ADMIN_IDS or user_id in activated_users:
        await update.message.reply_text("✅ Бот уже активирован для вас.")
        return ConversationHandler.END
    context.user_data["attempts"] = 0
    await update.message.reply_text("🔐 Введите секретный код (или /cancel для отмены):")
    return ENTER_SECRET_CODE

async def enter_secret_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    user_input = update.message.text.strip()
    if not user_input or len(user_input) > 50:
        await update.message.reply_text("❌ Код должен быть от 1 до 50 символов.")
        return ENTER_SECRET_CODE
    context.user_data["attempts"] = context.user_data.get("attempts", 0) + 1
    global_attempts = context.bot_data.setdefault('global_attempts', {})
    global_attempts[user_id] = global_attempts.get(user_id, 0) + 1
    if user_input == SECRET_CODE:
        context.bot_data['activated_users'].add(user_id)
        await context.bot.send_message(update.effective_chat.id, "✅ Бот активирован!")
        return ConversationHandler.END
    remaining_attempts = MAX_ATTEMPTS - global_attempts[user_id]
    if remaining_attempts > 0:
        await context.bot.send_message(update.effective_chat.id, f"❌ Неверный код. Осталось попыток: {remaining_attempts}.")
        return ENTER_SECRET_CODE
    await context.bot.send_message(update.effective_chat.id, "🚫 Превышено количество попыток.")
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("❌ Активация отменена")
    return ConversationHandler.END

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
async def health_check(context: ContextTypes.DEFAULT_TYPE) -> None:
    await context.bot.get_me()

async def error_handler(update: Optional[Update], context: ContextTypes.DEFAULT_TYPE) -> None:
    error = context.error
    logger.error(f"Ошибка: {error}", exc_info=error)
    if isinstance(error, (NetworkError, TimedOut)):
        logger.warning("Сетевая ошибка, перезапуск...")
        await restart_self(context)
    elif isinstance(error, BadRequest):
        logger.critical(f"Критическая ошибка Telegram API: {error}")
        await notify_admins(context, f"🚨 Критическая ошибка: {error}. Бот остановлен.")
        sys.exit(1)
    else:
        logger.error(f"Неизвестная ошибка, перезапуск...")
        await notify_admins(context, f"🚨 Неизвестная ошибка: {error}. Перезапуск бота.")
        await restart_self(context)

# Основной цикл
async def run_bot(application: Application) -> None:
    await init_db()
    application.bot_data['start_time'] = time.time()
    application.bot_data['restart_attempts'] = 0
    application.bot_data['messages_processed'] = 0
    application.bot_data['messages_today'] = 0
    application.bot_data['last_day_reset'] = get_current_time().date()
    application.bot_data['violations_cache'] = {}
    application.bot_data['subscriptions_cache'] = {}
    application.bot_data['banned_users'] = set()

    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, welcome_new_member))
    application.add_handler(CommandHandler("rules", show_rules))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("contacts", contacts_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("restart", restart_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Chat(GROUP_ID), check_message))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Chat(GROUP_ID), night_auto_reply))
    application.add_handler(CallbackQueryHandler(welcome_read_button, pattern="^welcome_read$"))
    application.add_handler(ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={ENTER_SECRET_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_secret_code)]},
        fallbacks=[CommandHandler("cancel", cancel)]
    ))
    application.add_error_handler(error_handler)
    application.job_queue.run_repeating(health_check, interval=21600, name="health_check")
    application.job_queue.run_repeating(sync_violations_cache, interval=SYNC_INTERVAL, name="sync_violations")
    application.job_queue.run_repeating(clean_violations_cache, interval=CLEAN_VIOLATIONS_INTERVAL, name="clean_violations")
    application.job_queue.run_repeating(heartbeat, interval=HEARTBEAT_INTERVAL, name="heartbeat")

async def post_init(application: Application) -> None:
    application.create_task(task_worker(application), name="task_worker")

async def main() -> None:
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    request = HTTPXRequest(
        connect_timeout=REQUEST_TIMEOUT,
        read_timeout=REQUEST_TIMEOUT,
        write_timeout=REQUEST_TIMEOUT,
        pool_timeout=REQUEST_TIMEOUT
    )
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .request(request)
        .concurrent_updates(True)
        .post_init(post_init)
        .build()
    )

    while True:
        try:
            await run_bot(app)
            await app.initialize()
            await app.start()
            await app.updater.start_polling(
                allowed_updates=Update.ALL_TYPES,
                timeout=30,
                drop_pending_updates=True,
                bootstrap_retries=5,
                error_callback=lambda e: logger.error(f"Ошибка polling: {e}")
            )
            logger.info("🤖 Бот успешно запущен!")

            shutdown_event = asyncio.Event()

            def signal_handler(sig: int, frame: Optional[object]) -> None:
                logger.info(f"Получен сигнал {sig}. Останавливаю бота...")
                print(f"[{get_current_time().strftime('%Y-%m-%d %H:%M:%S')}] Остановка бота по сигналу {sig}")
                shutdown_event.set()

            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)

            await shutdown_event.wait()

            await app.updater.stop()
            await app.stop()
            await app.shutdown()
            logger.info("Бот остановлен корректно.")
            print(f"[{get_current_time().strftime('%Y-%m-%d %H:%M:%S')}] Бот остановлен корректно.")
            break
        except Exception as e:
            logger.error(f"Критическая ошибка в основном цикле: {e}")
            await notify_admins(app, f"🚨 Критическая ошибка в основном цикле: {e}. Бот перезапускается.")
            await asyncio.sleep(RESTART_DELAY)
            await restart_self(app)

if __name__ == "__main__":
    asyncio.run(main())