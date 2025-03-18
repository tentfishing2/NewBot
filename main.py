import os
import sys
import asyncio
import signal
import time
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
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from functools import wraps
from loguru import logger
import subprocess
import re
import httpx
import psutil
from queue import PriorityQueue

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logger.remove()
logger.add("bot.log", rotation="1 MB", retention=5, level="INFO", encoding="utf-8", backtrace=True, diagnose=True)
logger.add(sys.stdout, level="INFO", format="{time} | {level} | {message}")

# Константы
MAX_ATTEMPTS = 3
ENTER_SECRET_CODE = 1
DB_TIMEOUT = 10
RESTART_DELAY = 60
MAX_VIOLATIONS = 3
CPU_LIMIT_SECONDS = 90
MIN_MESSAGE_LENGTH = 10
PING_INTERVAL = 1800  # Базовый интервал 30 минут
MIN_PING_INTERVAL = 300  # 5 минут
MAX_PING_INTERVAL = 3600  # 1 час
MAX_RESTART_ATTEMPTS = 3
SYNC_INTERVAL = 600  # Синхронизация кэша каждые 10 минут
CLEAN_VIOLATIONS_INTERVAL = 50 * 24 * 3600  # 50 дней в секундах
CPU_THRESHOLD = 80
RAM_THRESHOLD = 90

# Лимиты для rate limiting (в секундах)
RATE_LIMITS = {
    "default": 5,
    "start": 10,
    "rules": 5,
    "help": 5,
    "stats": 30,
    "restart": 60,
    "status": 30,
    "set_threshold": 60
}

# Переменные окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = set(map(int, filter(None, os.getenv("ADMIN_IDS", "").split(","))))
SECRET_CODE = os.getenv("SECRET_CODE")
GROUP_ID = int(os.getenv("GROUP_ID"))
CHANNEL_URL = os.getenv("CHANNEL_URL")
TIMEZONE = pytz.timezone(os.getenv("TIMEZONE", "UTC"))
WELCOME_MESSAGE_TIMEOUT = int(os.getenv("WELCOME_MESSAGE_TIMEOUT", 300))
VIOLATION_TIMEOUT_HOURS = int(os.getenv("VIOLATION_TIMEOUT_HOURS", 24))
NIGHT_START = int(os.getenv("NIGHT_AUTO_REPLY_START", 22))
NIGHT_END = int(os.getenv("NIGHT_AUTO_REPLY_END", 6))
OWNER_ID = int(os.getenv("OWNER_ID"))
PING_URL = "https://uptime.betterstack.com/api/v2/heartbeat/X7K9P2M5Q8N3B6J1"

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
    "🌟 <b>Команды:</b>\n\n"
    "• /start — активация бота;\n"
    "• /rules — правила сообщества;\n"
    "• /help — список команд;\n"
    "• /stats — статистика (для админов);\n"
    "• /status — состояние бота (для админов);\n"
    "• /restart — перезапуск бота (для админов);\n"
    "• /set_threshold — установка порогов CPU/RAM (для админов)."
)

BAD_WORDS_PATTERN = re.compile(
    r"\b(блять|сука|пиздец|хуй|ебать|пидор|мудак|долбоёб|хуёво|пизда|жопа|нахуй|говно|шлюха|хуесос|дебил|идиот|козёл|лох|мразь|тварь)\b",
    re.IGNORECASE
)

# Приоритетная очередь задач
task_queue = asyncio.PriorityQueue(maxsize=50)

# База данных и кэш
async def init_db() -> None:
    async with aiosqlite.connect("violations.db", timeout=DB_TIMEOUT) as conn:
        await conn.execute('''CREATE TABLE IF NOT EXISTS violations 
                            (user_id INTEGER PRIMARY KEY, count INTEGER, last_violation TEXT)''')
        await conn.commit()

async def load_violations_cache(context: ContextTypes.DEFAULT_TYPE):
    async with aiosqlite.connect("violations.db", timeout=DB_TIMEOUT) as conn:
        async with conn.execute("SELECT user_id, count, last_violation FROM violations") as cursor:
            rows = await cursor.fetchall()
            for user_id, count, last_violation in rows:
                context.bot_data['violations_cache'][user_id] = {
                    "count": count,
                    "last_violation": datetime.fromisoformat(last_violation) if last_violation else None
                }

async def sync_violations_cache(context: ContextTypes.DEFAULT_TYPE):
    try:
        async with aiosqlite.connect("violations.db", timeout=DB_TIMEOUT) as conn:
            for user_id, data in context.bot_data['violations_cache'].items():
                await conn.execute(
                    "INSERT OR REPLACE INTO violations (user_id, count, last_violation) VALUES (?, ?, ?)",
                    (user_id, data["count"], data["last_violation"].isoformat() if data["last_violation"] else None)
                )
            await conn.commit()
        logger.info("Кэш нарушений синхронизирован с базой данных")
    except Exception as e:
        logger.error(f"Ошибка синхронизации кэша: {e}")

async def clean_violations_cache(context: ContextTypes.DEFAULT_TYPE):
    now = get_current_time()
    cache = context.bot_data.get('violations_cache', {})
    removed = 0
    for user_id in list(cache.keys()):
        if now - cache[user_id]["last_violation"] > timedelta(hours=VIOLATION_TIMEOUT_HOURS):
            del cache[user_id]
            removed += 1
    logger.info(f"Кэш нарушений очищен, удалено {removed} устаревших записей")

async def get_violations(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> Dict[str, any]:
    cache = context.bot_data.get('violations_cache', {})
    return cache.get(user_id, {"count": 0, "last_violation": None})

async def update_violations(user_id: int, count: int, last_violation: datetime, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.bot_data.setdefault('violations_cache', {})[user_id] = {
        "count": count,
        "last_violation": last_violation
    }

# Вспомогательные функции
def get_current_time() -> datetime:
    return datetime.now(TIMEZONE)

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def is_night_time() -> bool:
    current_hour = get_current_time().hour
    return NIGHT_START <= current_hour < NIGHT_END

def rate_limit(command_name: str = "default"):
    def decorator(func):
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            user_id = update.effective_user.id
            current_time = asyncio.get_event_loop().time()
            limit = RATE_LIMITS.get(command_name, RATE_LIMITS["default"])
            last_command = context.bot_data.get(f"last_command_{user_id}_{command_name}", 0)
            if current_time - last_command < limit:
                await update.message.reply_text("⏳ Слишком много запросов. Подожди.")
                return
            context.bot_data[f"last_command_{user_id}_{command_name}"] = current_time
            return await func(update, context, *args, **kwargs)
        return wrapper
    return decorator

async def notify_admins(context: ContextTypes.DEFAULT_TYPE, message: str) -> None:
    for admin_id in ADMIN_IDS:
        await task_queue.put((1, lambda: context.bot.send_message(chat_id=admin_id, text=message, parse_mode="HTML")))

def create_subscribe_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("👉 ПОДПИСАТЬСЯ НА КАНАЛ 👈", url=CHANNEL_URL)]])

# Мониторинг ресурсов
def check_resources():
    cpu_usage = psutil.cpu_percent(interval=1)
    ram_usage = psutil.virtual_memory().percent
    return cpu_usage < CPU_THRESHOLD and ram_usage < RAM_THRESHOLD

# Отслеживание CPU
def track_cpu_time(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.process_time()
        result = await func(*args, **kwargs)
        end_time = time.process_time()
        cpu_time = end_time - start_time
        context = args[1] if len(args) > 1 else kwargs.get('context')
        if context:
            context.bot_data['cpu_used'] = context.bot_data.get('cpu_used', 0.0) + cpu_time
        return result
    return wrapper

# Асинхронный воркер для приоритетной очереди задач
async def task_worker(context: ContextTypes.DEFAULT_TYPE):
    while True:
        priority, task = await task_queue.get()
        try:
            await task()
        except (NetworkError, TimedOut):
            logger.warning(f"Сетевая ошибка в задаче с приоритетом {priority}")
        except Exception as e:
            logger.error(f"Ошибка в задаче с приоритетом {priority}: {e}")
        finally:
            task_queue.task_done()

# Проверка дублирования процессов
def check_duplicate_process():
    result = subprocess.run(['pgrep', '-f', f'python3 {os.path.abspath(__file__)}'], capture_output=True, text=True)
    pids = result.stdout.strip().split()
    current_pid = str(os.getpid())
    return len(pids) > 1 and current_pid in pids

# Перезапуск и пинг
async def restart_self(context: ContextTypes.DEFAULT_TYPE = None):
    restart_attempts = context.bot_data.get('restart_attempts', 0) if context else 0
    restart_attempts += 1
    if restart_attempts > MAX_RESTART_ATTEMPTS:
        logger.critical(f"Превышено максимальное количество перезапусков ({MAX_RESTART_ATTEMPTS}). Требуется ручной перезапуск.")
        if context:
            await notify_admins(context, f"🚨 Бот остановлен: превышено максимальное количество перезапусков ({MAX_RESTART_ATTEMPTS}). Перезапустите вручную.")
        sys.exit(1)
    if context:
        context.bot_data['restart_attempts'] = restart_attempts
    if check_duplicate_process():
        logger.warning("Обнаружен дублирующий процесс, завершаю текущий экземпляр")
        sys.exit(0)
    try:
        logger.info(f"Инициирую перезапуск бота (попытка {restart_attempts}/{MAX_RESTART_ATTEMPTS})...")
        subprocess.Popen(['python3', os.path.abspath(__file__)], env=os.environ.copy())
        await asyncio.sleep(2)
        sys.exit(0)
    except Exception as e:
        logger.error(f"Ошибка при перезапуске: {e}")
        await asyncio.sleep(RESTART_DELAY * (2 ** min(restart_attempts, 5)))
        await restart_self(context)

async def initial_ping(context: ContextTypes.DEFAULT_TYPE):
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=15.0)) as client:
        for attempt in range(5):
            try:
                response = await client.get(PING_URL)
                if response.status_code == 200:
                    logger.info("Первый пинг успешен, активирую регулярные пинги")
                    context.bot_data['ping_enabled'] = True
                    context.job_queue.run_repeating(
                        ping_uptime,
                        interval=lambda ctx: ctx.bot_data.get('ping_interval', PING_INTERVAL),
                        first=10
                    )
                    return True
                else:
                    logger.warning(f"Первый пинг не удался: {response.status_code}")
            except Exception as e:
                logger.error(f"Ошибка первого пинга: {e}")
            await asyncio.sleep(10 * (2 ** attempt))
    logger.critical("Не удалось выполнить первый пинг после 5 попыток, пинг не активирован")
    await notify_admins(context, "🚨 Не удалось выполнить первый пинг после 5 попыток. Регулярные пинги не активированы.")
    return False

@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=60), retry=retry_if_exception(Exception))
@track_cpu_time
async def ping_uptime(context: ContextTypes.DEFAULT_TYPE):
    if not context.bot_data.get('ping_enabled', False) or not check_resources():
        logger.warning("Пинг отключен или высокая нагрузка, пропускаю")
        return
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=15.0)) as client:
        try:
            response = await client.get(PING_URL)
            if response.status_code == 200:
                logger.info("Пинг до Better Uptime успешен")
                current_interval = context.bot_data.get('ping_interval', PING_INTERVAL)
                context.bot_data['ping_interval'] = min(current_interval + 300, MAX_PING_INTERVAL)
            else:
                logger.warning(f"Ошибка пинга Better Uptime: {response.status_code}")
                raise Exception("Неудачный пинг")
        except Exception as e:
            logger.error(f"Не удалось выполнить пинг: {e}")
            current_interval = context.bot_data.get('ping_interval', PING_INTERVAL)
            context.bot_data['ping_interval'] = max(current_interval - 300, MIN_PING_INTERVAL)
            await restart_self(context)

# Обработчики
@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=60), retry=retry_if_exception(lambda e: isinstance(e, (NetworkError, TimedOut))))
@track_cpu_time
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
        try:
            group_msg = await context.bot.send_message(
                chat_id=GROUP_ID,
                text=WELCOME_TEXT.format(name=name),
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=keyboard
            )
            context.job_queue.run_once(
                lambda ctx: asyncio.create_task(ctx.bot.delete_message(chat_id=GROUP_ID, message_id=group_msg.message_id)),
                WELCOME_MESSAGE_TIMEOUT
            )
            logger.info(f"Приветствие для {name} ({member.id}) в группе")
        except NetworkError:
            logger.warning("Сетевая ошибка при отправке приветствия")
        except TimedOut:
            logger.warning("Тайм-аут при отправке приветствия")
        except BadRequest as e:
            logger.error(f"Неверный запрос при отправке приветствия: {e}")
        except Exception as e:
            logger.critical(f"Неизвестная ошибка при отправке приветствия: {e}")

@track_cpu_time
async def welcome_read_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    try:
        await query.answer()
        await query.message.delete()
    except NetworkError:
        logger.warning("Сетевая ошибка при обработке кнопки")
    except BadRequest as e:
        logger.error(f"Неверный запрос при обработке кнопки: {e}")

@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=60), retry=retry_if_exception(lambda e: isinstance(e, (NetworkError, TimedOut))))
@track_cpu_time
async def night_auto_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat_id != GROUP_ID or not update.message.text or not is_night_time() or not check_resources():
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
    await task_queue.put((2, lambda: update.message.reply_text(response, parse_mode="HTML", reply_markup=keyboard)))
    await task_queue.put((1, lambda: context.bot.send_message(chat_id=OWNER_ID, text=f"🔔 Ночное сообщение от {user_name} (ID: {user_id}): {text}", parse_mode="HTML")))

@track_cpu_time
async def check_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat_id != GROUP_ID or not update.message.text or is_admin(update.effective_user.id):
        return
    text = update.message.text
    if len(text) < MIN_MESSAGE_LENGTH or not BAD_WORDS_PATTERN.search(text):
        return
    user_id = update.effective_user.id
    now = get_current_time()
    violation_data = await get_violations(user_id, context)
    count = 0 if not violation_data["last_violation"] or (now - violation_data["last_violation"]) > timedelta(hours=VIOLATION_TIMEOUT_HOURS) else violation_data["count"]
    count += 1
    await update_violations(user_id, count, now, context)
    try:
        bot_rights = await context.bot.get_chat_member(chat_id=GROUP_ID, user_id=context.bot.id)
        if bot_rights.can_delete_messages:
            await update.message.delete()
        remaining_lives = MAX_VIOLATIONS - count
        keyboard = create_subscribe_keyboard()
        await task_queue.put((2, lambda: context.bot.send_message(
            chat_id=GROUP_ID,
            text=f"⚠️ Нарушение правил! Осталось предупреждений: {remaining_lives}",
            parse_mode="HTML",
            reply_markup=keyboard
        )))
        if count >= MAX_VIOLATIONS and bot_rights.can_restrict_members:
            await context.bot.ban_chat_member(GROUP_ID, user_id)
            await task_queue.put((2, lambda: context.bot.send_message(
                chat_id=GROUP_ID,
                text="🚫 Пользователь заблокирован.",
                reply_markup=keyboard
            )))
    except NetworkError:
        logger.warning("Сетевая ошибка при проверке сообщения")
    except BadRequest as e:
        logger.error(f"Неверный запрос при проверке сообщения: {e}")

@rate_limit("rules")
@track_cpu_time
async def show_rules(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = create_subscribe_keyboard()
    await update.message.reply_text(RULES_TEXT, parse_mode="HTML", reply_markup=keyboard)

@rate_limit("help")
@track_cpu_time
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = create_subscribe_keyboard()
    await update.message.reply_text(HELP_TEXT, parse_mode="HTML", reply_markup=keyboard)

@rate_limit("stats")
@track_cpu_time
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("🚫 Команда только для админов!")
        return
    violations = context.bot_data.get('violations_cache', {})
    if not violations:
        await update.message.reply_text("📊 Нарушений нет.")
        return
    message = "📊 <b>Статистика:</b>\n" + "\n".join(f"ID {user_id}: {data['count']} нарушений" for user_id, data in violations.items())
    await update.message.reply_text(message, parse_mode="HTML")

@rate_limit("restart")
@track_cpu_time
async def restart_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("🚫 Команда только для админов!")
        return
    await update.message.reply_text("🔄 Перезапуск бота...")
    context.bot_data['restart_attempts'] = 0
    await restart_self(context)

@rate_limit("status")
@track_cpu_time
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("🚫 Команда только для админов!")
        return
    uptime = time.time() - context.bot_data.get('start_time', time.time())
    cpu_usage = psutil.cpu_percent(interval=1)
    ram_usage = psutil.virtual_memory().percent
    messages_processed = context.bot_data.get('messages_processed', 0)
    restarts = context.bot_data.get('restart_attempts', 0)
    ping_status = "Активен" if context.bot_data.get('ping_enabled', False) else "Не активен"
    status_text = (
        f"📈 <b>Состояние бота:</b>\n"
        f"⏳ Время работы: {int(uptime // 3600)}ч {int((uptime % 3600) // 60)}м\n"
        f"📩 Обработано сообщений: {messages_processed}\n"
        f"💻 CPU: {cpu_usage:.1f}%\n"
        f"🧠 RAM: {ram_usage:.1f}%\n"
        f"🔄 Перезапусков: {restarts}\n"
        f"📡 Пинг: {ping_status}"
    )
    await update.message.reply_text(status_text, parse_mode="HTML")

@rate_limit("set_threshold")
@track_cpu_time
async def set_threshold(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("🚫 Команда только для админов!")
        return
    try:
        cpu, ram = map(float, context.args)
        global CPU_THRESHOLD, RAM_THRESHOLD
        CPU_THRESHOLD, RAM_THRESHOLD = cpu, ram
        await update.message.reply_text(f"Пороги изменены: CPU={cpu}%, RAM={ram}%")
    except ValueError:
        await update.message.reply_text("🚫 Укажите два числа: /set_threshold <cpu> <ram>")

@rate_limit("start")
@track_cpu_time
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    activated_users = context.bot_data.setdefault('activated_users', set())
    if user_id in ADMIN_IDS or user_id in activated_users:
        await update.message.reply_text("✅ Бот уже активирован для вас.")
        return ConversationHandler.END
    context.user_data["attempts"] = 0
    await update.message.reply_text("🔐 Введите секретный код (или /cancel для отмены):")
    return ENTER_SECRET_CODE

@track_cpu_time
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

@track_cpu_time
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("❌ Активация отменена.")
    return ConversationHandler.END

@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=60), retry=retry_if_exception(lambda e: isinstance(e, (NetworkError, TimedOut))))
@track_cpu_time
async def health_check(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_resources():
        logger.warning("Высокая нагрузка, пропускаю проверку здоровья")
        return
    try:
        await context.bot.get_me()
        logger.info("Бот жив")
    except (NetworkError, TimedOut):
        logger.error("Сетевая ошибка при проверке здоровья")
        await restart_self(context)
    except BadRequest as e:
        logger.critical(f"Критическая ошибка Telegram API: {e}")
        await notify_admins(context, f"🚨 Критическая ошибка: {e}. Бот остановлен.")
        sys.exit(1)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Ошибка: {context.error}", exc_info=context.error)
    if isinstance(context.error, (NetworkError, TimedOut)):
        logger.warning("Сетевая ошибка, перезапуск...")
        await restart_self(context)
    elif isinstance(context.error, BadRequest):
        logger.critical(f"Критическая ошибка Telegram API: {context.error}")
        await notify_admins(context, f"🚨 Критическая ошибка: {context.error}. Бот остановлен.")
        sys.exit(1)
    else:
        await notify_admins(context, f"🚨 Неизвестная ошибка: {context.error}")

# Основной цикл
async def run_bot():
    global app
    request = HTTPXRequest(connect_timeout=120, read_timeout=120, pool_timeout=120, write_timeout=120)
    app = ApplicationBuilder().token(BOT_TOKEN).request(request).concurrent_updates(True).build()
    await init_db()
    app.bot_data['start_time'] = time.time()
    app.bot_data['restart_attempts'] = 0
    app.bot_data['messages_processed'] = 0
    app.bot_data['violations_cache'] = {}
    app.bot_data['ping_interval'] = PING_INTERVAL
    app.bot_data['ping_enabled'] = False
    await load_violations_cache(app)

    # Обработчики
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, welcome_new_member))
    app.add_handler(CommandHandler("rules", show_rules))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("restart", restart_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("set_threshold", set_threshold))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Chat(GROUP_ID), check_message))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Chat(GROUP_ID), night_auto_reply))
    app.add_handler(CallbackQueryHandler(welcome_read_button, pattern="^welcome_read$"))
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={ENTER_SECRET_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_secret_code)]},
        fallbacks=[CommandHandler("cancel", cancel)]
    ))
    app.add_error_handler(error_handler)
    app.job_queue.run_repeating(health_check, interval=21600)
    app.job_queue.run_repeating(sync_violations_cache, interval=SYNC_INTERVAL)
    app.job_queue.run_repeating(clean_violations_cache, interval=CLEAN_VIOLATIONS_INTERVAL)

    # Запуск воркера очереди задач
    asyncio.create_task(task_worker(app))

    # Инициализация первого пинга
    await initial_ping(app)

    try:
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
        await notify_admins(app, "🤖 Бот успешно запущен!")
    except (NetworkError, TimedOut):
        logger.error("Сетевая ошибка при запуске")
        await restart_self(app)
    except BadRequest as e:
        logger.critical(f"Критическая ошибка при запуске: {e}")
        await notify_admins(app, f"🚨 Критическая ошибка при запуске: {e}")
        sys.exit(1)

    shutdown_event = asyncio.Event()

    def signal_handler(sig, frame):
        logger.info(f"Получен сигнал {sig} (Ctrl+C или SIGTERM). Останавливаю бота...")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Остановка бота по сигналу {sig}")
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    await shutdown_event.wait()

    try:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        logger.info("Бот остановлен корректно.")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Бот остановлен корректно.")
    except Exception as e:
        logger.error(f"Ошибка остановки бота: {e}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Ошибка при остановке бота: {e}")

async def main():
    while True:
        try:
            await run_bot()
            break
        except (NetworkError, TimedOut):
            logger.error("Сетевая ошибка в основном цикле, перезапуск...")
            await asyncio.sleep(RESTART_DELAY)
            await restart_self()
        except BadRequest as e:
            logger.critical(f"Критическая ошибка в основном цикле: {e}")
            sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())