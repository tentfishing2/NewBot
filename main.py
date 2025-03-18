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

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logger.remove()
logger.add("bot.log", rotation="1 MB", level="INFO", encoding="utf-8", backtrace=True, diagnose=True)
logger.add(sys.stdout, level="INFO", format="{time} | {level} | {message}")  # Вывод в консоль

# Константы
MAX_ATTEMPTS = 3
ENTER_SECRET_CODE = 1
DB_TIMEOUT = 10
RESTART_DELAY = 60
MAX_VIOLATIONS = 3
CPU_LIMIT_SECONDS = 90
MIN_MESSAGE_LENGTH = 10
PING_INTERVAL = 900  # 15 минут
PING_URL = "https://uptime.betterstack.com/api/v2/heartbeat/X7K9P2M5Q8N3B6J1"

# Лимиты для rate limiting (в секундах)
RATE_LIMITS = {
    "default": 5,    # Лимит по умолчанию: 5 секунд между командами
    "start": 10,     # Лимит для /start: 10 секунд
    "rules": 5,      # Лимит для /rules: 5 секунд
    "help": 5,       # Лимит для /help: 5 секунд
    "stats": 30      # Лимит для /stats: 30 секунд (для админов)
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

if not all([BOT_TOKEN, SECRET_CODE, CHANNEL_URL]):
    logger.critical("Отсутствуют обязательные переменные окружения!")
    raise SystemExit(1)

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
    "• /stats — статистика (для админов)."
)

BAD_WORDS_PATTERN = re.compile(
    r"\b(блять|сука|пиздец|хуй|ебать|пидор|мудак|долбоёб|хуёво|пизда|жопа|нахуй|говно|шлюха|хуесос|дебил|идиот|козёл|лох|мразь|тварь)\b",
    re.IGNORECASE
)

# База данных
async def init_db() -> None:
    async with aiosqlite.connect("violations.db", timeout=DB_TIMEOUT) as conn:
        await conn.execute('''CREATE TABLE IF NOT EXISTS violations 
                            (user_id INTEGER PRIMARY KEY, count INTEGER, last_violation TEXT)''')
        await conn.commit()

async def get_violations(user_id: int) -> Dict[str, any]:
    try:
        async with aiosqlite.connect("violations.db", timeout=DB_TIMEOUT) as conn:
            async with conn.execute("SELECT count, last_violation FROM violations WHERE user_id = ?", (user_id,)) as cursor:
                result = await cursor.fetchone()
                return {"count": result[0], "last_violation": datetime.fromisoformat(result[1]) if result[1] else None} if result else {"count": 0, "last_violation": None}
    except Exception as e:
        logger.error(f"Ошибка получения нарушений: {e}")
        return {"count": 0, "last_violation": None}

async def update_violations(user_id: int, count: int, last_violation: datetime) -> None:
    try:
        async with aiosqlite.connect("violations.db", timeout=DB_TIMEOUT) as conn:
            await conn.execute("INSERT OR REPLACE INTO violations (user_id, count, last_violation) VALUES (?, ?, ?)",
                              (user_id, count, last_violation.isoformat()))
            await conn.commit()
    except Exception as e:
        logger.error(f"Ошибка обновления нарушений: {e}")

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
        try:
            await context.bot.send_message(chat_id=admin_id, text=message, parse_mode="HTML")
        except Exception as e:
            logger.warning(f"Не удалось уведомить админа {admin_id}: {e}")

def create_subscribe_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👉 ПОДПИСАТЬСЯ НА КАНАЛ 👈", url=CHANNEL_URL)]
    ])

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

# Автоматический перезапуск и пинг
def is_bot_running():
    try:
        result = subprocess.run(
            ['pgrep', '-f', f'python3 {os.path.abspath(__file__)}'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        pids = result.stdout.strip().split()
        current_pid = str(os.getpid())
        return len(pids) > 1 or (len(pids) == 1 and pids[0] != current_pid)
    except Exception as e:
        logger.error(f"Ошибка проверки процессов: {e}")
        return False

async def restart_self():
    try:
        logger.info("Инициирую перезапуск бота...")
        subprocess.Popen(['python3', os.path.abspath(__file__)], env=os.environ.copy())
        await asyncio.sleep(2)
        sys.exit(0)
    except Exception as e:
        logger.error(f"Ошибка при перезапуске: {e}")
        await asyncio.sleep(RESTART_DELAY)
        await restart_self()

@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=60), retry=retry_if_exception(Exception))
@track_cpu_time
async def ping_uptime(context: ContextTypes.DEFAULT_TYPE):
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=15.0)) as client:
        try:
            response = await client.get(PING_URL)
            if response.status_code == 200:
                logger.info("Пинг до Better Uptime успешен")
            else:
                logger.warning(f"Ошибка пинга Better Uptime: {response.status_code}")
                raise Exception("Неудачный пинг, перезапуск...")
        except Exception as e:
            logger.error(f"Не удалось выполнить пинг: {e}")
            await restart_self()

# Обработчики
@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=60), retry=retry_if_exception(lambda e: isinstance(e, (NetworkError, TimedOut, BadRequest))))
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
        except Exception as e:
            logger.error(f"Ошибка отправки приветствия: {e}")
            await asyncio.sleep(5)

@track_cpu_time
async def welcome_read_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    try:
        await query.answer()
        await query.message.delete()
    except Exception as e:
        logger.error(f"Ошибка обработки кнопки: {e}")

@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=60), retry=retry_if_exception(lambda e: isinstance(e, (NetworkError, TimedOut, BadRequest))))
@track_cpu_time
async def night_auto_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat_id != GROUP_ID or not update.message.text:
        return
    if not is_night_time():
        return
    if context.bot_data.get('cpu_used', 0.0) > CPU_LIMIT_SECONDS:
        logger.warning("Пропуск ночного ответа из-за превышения CPU")
        return
    now = get_current_time()
    user_name = update.message.from_user.first_name
    user_id = update.message.from_user.id
    text = update.message.text if len(update.message.text) <= 4096 else "Сообщение слишком длинное"
    response = (
        f"🌟 Здравствуйте, {user_name}! 🌟 Это ночной автоответчик 🌙✨\n"
        f"🌙 Наша команда — <b>Палатки-ДВ</b> уже отдыхает, так как у нас ночь ({now.strftime('%H:%M')}). 🛌💤\n"
        "🌄 С первыми утренними лучами мы обязательно вам ответим! 🌅✨\n"
        "🙏 Спасибо за ваше терпение и понимание! 💫"
    )
    keyboard = create_subscribe_keyboard()
    try:
        sent_message = await update.message.reply_text(response, parse_mode="HTML", reply_markup=keyboard)
        await context.bot.send_message(
            chat_id=OWNER_ID,
            text=f"🔔 Ночное сообщение от {user_name} (ID: {user_id}): {text}",
            parse_mode="HTML"
        )
        next_deletion = now.replace(hour=11, minute=0, second=0, microsecond=0)
        if now >= next_deletion:
            next_deletion += timedelta(days=1)
        context.job_queue.run_once(
            lambda ctx: asyncio.create_task(ctx.bot.delete_message(chat_id=sent_message.chat_id, message_id=sent_message.message_id)),
            (next_deletion - now).total_seconds()
        )
        logger.info(f"Ночной ответ для {user_id}")
    except Exception as e:
        logger.error(f"Ошибка ночного ответа: {e}")

@track_cpu_time
async def check_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat_id != GROUP_ID or not update.message.text or is_admin(update.effective_user.id):
        return
    text = update.message.text
    if len(text) < MIN_MESSAGE_LENGTH:
        return
    if BAD_WORDS_PATTERN.search(text):
        user_id = update.effective_user.id
        now = get_current_time()
        violation_data = await get_violations(user_id)
        count = 0 if not violation_data["last_violation"] or (now - violation_data["last_violation"]) > timedelta(hours=VIOLATION_TIMEOUT_HOURS) else violation_data["count"]
        count += 1
        await update_violations(user_id, count, now)
        try:
            bot_rights = await context.bot.get_chat_member(chat_id=GROUP_ID, user_id=context.bot.id)
            if bot_rights.can_delete_messages:
                await update.message.delete()
            remaining_lives = MAX_VIOLATIONS - count
            keyboard = create_subscribe_keyboard()
            await context.bot.send_message(
                chat_id=GROUP_ID,
                text=f"⚠️ Нарушение правил! Осталось предупреждений: {remaining_lives}",
                parse_mode="HTML",
                reply_markup=keyboard
            )
            if count >= MAX_VIOLATIONS and bot_rights.can_restrict_members:
                await context.bot.ban_chat_member(GROUP_ID, user_id)
                await context.bot.send_message(GROUP_ID, f"🚫 Пользователь заблокирован.", reply_markup=keyboard)
        except Exception as e:
            logger.error(f"Ошибка проверки сообщения: {e}")

@rate_limit("rules")
@track_cpu_time
async def show_rules(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = create_subscribe_keyboard()
    try:
        await update.message.reply_text(RULES_TEXT, parse_mode="HTML", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Ошибка вывода правил: {e}")

@rate_limit("help")
@track_cpu_time
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = create_subscribe_keyboard()
    try:
        await update.message.reply_text(HELP_TEXT, parse_mode="HTML", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Ошибка вывода помощи: {e}")

@rate_limit("stats")
@track_cpu_time
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        try:
            await update.message.reply_text("🚫 Команда только для админов!")
        except Exception as e:
            logger.error(f"Ошибка проверки админа: {e}")
        return
    try:
        async with aiosqlite.connect("violations.db", timeout=DB_TIMEOUT) as conn:
            async with conn.execute("SELECT user_id, count FROM violations") as cursor:
                violations = await cursor.fetchall()
        if not violations:
            await update.message.reply_text("📊 Нарушений нет.")
            return
        message = "📊 <b>Статистика:</b>\n"
        for user_id, count in violations:
            message += f"ID {user_id}: {count} нарушений\n"
        await update.message.reply_text(message, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Ошибка вывода статистики: {e}")

@rate_limit("start")
@track_cpu_time
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    activated_users = context.bot_data.setdefault('activated_users', set())
    if user_id in ADMIN_IDS or user_id in activated_users:
        try:
            await update.message.reply_text("✅ Бот уже активирован для вас.")
            return ConversationHandler.END
        except Exception as e:
            logger.error(f"Ошибка активации: {e}")
    context.user_data["attempts"] = 0
    try:
        await update.message.reply_text("🔐 Введите секретный код (или /cancel для отмены):")
        return ENTER_SECRET_CODE
    except Exception as e:
        logger.error(f"Ошибка ввода кода: {e}")
        return ConversationHandler.END

@track_cpu_time
async def enter_secret_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    user_input = update.message.text.strip()
    if not user_input or len(user_input) > 50:
        try:
            await update.message.reply_text("❌ Код должен быть от 1 до 50 символов.")
            return ENTER_SECRET_CODE
        except Exception as e:
            logger.error(f"Ошибка проверки кода: {e}")
            return ENTER_SECRET_CODE
    context.user_data["attempts"] = context.user_data.get("attempts", 0) + 1
    global_attempts = context.bot_data.setdefault('global_attempts', {})
    global_attempts[user_id] = global_attempts.get(user_id, 0) + 1
    try:
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
    except Exception as e:
        logger.error(f"Ошибка обработки кода: {e}")
        return ConversationHandler.END

@track_cpu_time
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        await update.message.reply_text("❌ Активация отменена.")
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Ошибка отмены: {e}")
        return ConversationHandler.END

@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=60), retry=retry_if_exception(lambda e: isinstance(e, (NetworkError, TimedOut))))
@track_cpu_time
async def health_check(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        await context.bot.get_me()
        logger.info("Бот жив")
    except Exception as e:
        logger.error(f"Ошибка проверки здоровья: {e}")
        await restart_self()

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Ошибка: {context.error}", exc_info=context.error)
    await notify_admins(context, f"🚨 Ошибка: {context.error}")
    if isinstance(context.error, (NetworkError, TimedOut, BadRequest, TelegramError)):
        logger.warning("Сетевая ошибка или ошибка Telegram, инициирую перезапуск...")
        await restart_self()

# Основной цикл бота
async def run_bot():
    global app
    request = HTTPXRequest(connect_timeout=120, read_timeout=120, pool_timeout=120, write_timeout=120)
    app = ApplicationBuilder().token(BOT_TOKEN).request(request).concurrent_updates(True).build()
    await init_db()

    # Добавление обработчиков
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, welcome_new_member))
    app.add_handler(CommandHandler("rules", show_rules))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("stats", stats_command))
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
    app.job_queue.run_repeating(ping_uptime, interval=PING_INTERVAL)

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
        logger.info("🤖 Бот запущен в режиме polling!")
    except Exception as e:
        logger.critical(f"Ошибка запуска бота: {e}")
        await restart_self()

    # Ожидание завершения
    shutdown_event = asyncio.Event()

    def signal_handler(sig, frame):
        logger.info(f"Получен сигнал {sig}. Завершение работы...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    await shutdown_event.wait()

    # Остановка
    try:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        logger.info("Бот остановлен корректно.")
    except Exception as e:
        logger.error(f"Ошибка остановки бота: {e}")

async def main():
    while True:
        try:
            await run_bot()
            break
        except Exception as e:
            logger.critical(f"Критическая ошибка: {e}. Перезапуск через {RESTART_DELAY} секунд...")
            await asyncio.sleep(RESTART_DELAY)
            await restart_self()

if __name__ == "__main__":
    asyncio.run(main())