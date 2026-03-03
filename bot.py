import os
import json
import re
import logging
import asyncio
from pathlib import Path
from datetime import datetime, timedelta

from dotenv import load_dotenv
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# =====================================================
# ENV
# =====================================================
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = (os.getenv("ADMIN_ID") or "").strip()
ADMIN_ID_INT = int(ADMIN_ID) if ADMIN_ID.isdigit() else None

if not TELEGRAM_BOT_TOKEN:
    print("❌ TELEGRAM_BOT_TOKEN не найден")
    exit(1)

MBANK_REKV_FALLBACK = os.getenv("MBANK_REKV", "")

# Реквизиты для оплаты (всегда показываем в тексте оплаты)
PAYMENT_REKV_TEXT = (
    "💳 Оплата (MBank)\n"
    "Номер: +996999888332\n"
    "Получатель: Имронбек С.\n"
    "После оплаты отправьте чек (скрин/фото), чтобы было видно сумму и дату/время."
)


# =====================================================
# JSON helpers
# =====================================================
def load_json(filepath, default=None):
    """Загружает JSON из файла, если файл не существует или ошибка - возвращает default"""
    path = Path(filepath)
    if not path.exists():
        return default if default is not None else {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default if default is not None else {}

def save_json(filepath, data):
    """Сохраняет данные в JSON файл"""
    path = Path(filepath)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

# =====================================================
# BAN SYSTEM
# =====================================================
BAN_PATH = Path("bans.json")

def load_bans():
    if not BAN_PATH.exists():
        return {}
    try:
        return json.loads(BAN_PATH.read_text(encoding="utf-8"))
    except:
        return {}

def save_bans(data):
    BAN_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

BANS = load_bans()

BAN_STEPS = [5, 10, 15, 30, 45, 60]  # минуты

# =====================================================
# ANTI-SPAM
# =====================================================
SPAM_TRACKER = {}

SPAM_LIMIT = 5       # сообщений
SPAM_SECONDS = 10    # за сколько секунд

# =====================================================
# Knowledge Base (kb_index.json)
# =====================================================
KB_PATH = Path("kb_index.json")

def load_kb_items():
    if not KB_PATH.exists():
        return []
    try:
        data = json.loads(KB_PATH.read_text(encoding="utf-8"))
        return data.get("items", [])
    except Exception:
        return []

KB_ITEMS = load_kb_items()

def get_doc_by_name(filename: str) -> str:
    parts = [it.get("text", "") for it in KB_ITEMS if it.get("source") == filename]
    return ("\n\n".join(parts)).strip()


def get_payment_text() -> str:
    """Возвращает текст оплаты. Реквизиты всегда включены."""
    extra = (get_doc_by_name("payment.txt") or MBANK_REKV_FALLBACK or "").strip()
    if extra:
        # если в knowledge уже прописали реквизиты — всё равно оставим наши вверху
        return PAYMENT_REKV_TEXT + "\n\n" + extra
    return PAYMENT_REKV_TEXT

# =====================================================
# Orders storage (orders.json)
# =====================================================
ORDERS_PATH = Path("orders.json")

def load_orders():
    if not ORDERS_PATH.exists():
        return {"last_id": 0, "orders": {}}
    try:
        return json.loads(ORDERS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"last_id": 0, "orders": {}}

def save_orders(db):
    ORDERS_PATH.write_text(json.dumps(db, ensure_ascii=False, indent=2), encoding="utf-8")

ORDERS_DB = load_orders()

def new_order_id() -> int:
    ORDERS_DB["last_id"] = int(ORDERS_DB.get("last_id", 0)) + 1
    save_orders(ORDERS_DB)
    return ORDERS_DB["last_id"]

def now_iso():
    return datetime.now().isoformat(timespec="seconds")

def parse_iso(dt_str: str):
    try:
        return datetime.fromisoformat(dt_str)
    except Exception:
        return None


# =====================================================
# Deadline parsing + Urgency (<=4h => +100, >5h => 0)
# =====================================================
def parse_deadline_text(text: str):
    """Поддержка: HH:MM (сегодня), 'сегодня 15:00', 'завтра 10:00', 'через 3 часа', YYYY-MM-DD, YYYY-MM-DD HH:MM"""
    if not text:
        return None
    t = text.strip().lower()
    now = datetime.now()

    m = re.search(r"через\s*(\d+)\s*(час|часа|часов|день|дня|дней)", t)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        return now + (timedelta(hours=n) if "час" in unit else timedelta(days=n))

    m = re.search(r"^(сегодня|завтра)\s+(\d{1,2}):(\d{2})$", t)
    if m:
        day = m.group(1)
        hh = int(m.group(2))
        mm = int(m.group(3))
        base = now if day == "сегодня" else (now + timedelta(days=1))
        return base.replace(hour=hh, minute=mm, second=0, microsecond=0)

    m = re.fullmatch(r"(\d{1,2}):(\d{2})", t)
    if m:
        hh = int(m.group(1))
        mm = int(m.group(2))
        return now.replace(hour=hh, minute=mm, second=0, microsecond=0)

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", t):
        try:
            d = datetime.fromisoformat(t)
            return d.replace(hour=23, minute=59, second=0, microsecond=0)
        except Exception:
            return None

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}", t):
        try:
            return datetime.fromisoformat(t.replace(" ", "T"))
        except Exception:
            return None

    return None

def urgency_fee_from_deadline(deadline_text: str):
    """Если до дедлайна <=4 часа => +100. Если >5 часов => 0. (4-5 часов тоже 0)"""
    dt = parse_deadline_text(deadline_text)
    if not dt:
        return False, 0, None
    hours_left = (dt - datetime.now()).total_seconds() / 3600
    if hours_left <= 4:
        return True, 100, int(hours_left)
    return False, 0, int(hours_left)

# =====================================================
# Users storage (users.json)
# =====================================================
USERS_PATH = Path("users.json")

def load_users():
    if not USERS_PATH.exists():
        return []
    try:
        return json.loads(USERS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []

def save_users(users):
    USERS_PATH.write_text(json.dumps(users, ensure_ascii=False, indent=2), encoding="utf-8")

USERS_DB = load_users()

# =====================================================
# Products / Catalog
# =====================================================
PRODUCTS_PATH = Path("products.json")

def load_products():
    if not PRODUCTS_PATH.exists():
        # Дефолтные товары
        default_products = {
            "gistology_ready": {
                "title": "📚 СРС по гистологии (1–2 модуль) — комплект",
                "type": "ready",
                "price": 499,
                "delivery_doc": "delivery_gistology_ready.txt",
            },
            "kahoot": {"title": "🧠 Kahoot (индивидуально)", "type": "individual"},
            "srs": {"title": "📚 СРС (самостоятельная работа)", "type": "individual"},
            "referat": {"title": "📄 Реферат", "type": "individual"},
            "doklad": {"title": "📘 Доклад", "type": "individual"},
            "presentation": {"title": "📊 Презентация (PowerPoint)", "type": "individual"},
        }
        save_json(PRODUCTS_PATH, default_products)
        return default_products
    return load_json(PRODUCTS_PATH, {})

PRODUCTS = load_products()

# =====================================================
# Pricing templates (авторасчёт)
# =====================================================
PRICING_RULES = {
    "kahoot": ("вопрос", 10, 300),
    "srs": ("страница", 35, 400),
    "referat": ("страница", 40, 500),
    "doklad": ("страница", 30, 300),
    "presentation": ("слайд", 50, 400),
}

# =====================================================
# PROMO CODES (persisted)
# =====================================================
PROMO_PATH = Path("promo.json")
PROMO_CODES = load_json(PROMO_PATH, {})
# встроенный авто промо на 5%: можно вводить "5" или "5%"
if "AUTO5" not in PROMO_CODES:
    PROMO_CODES["AUTO5"] = {"discount": 5, "expires": None, "limit": 10**9, "used": 0}
    save_json(PROMO_PATH, PROMO_CODES)

def _parse_expire(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def validate_promo(code: str):
    """returns (discount:int|None, error:str|None)"""
    if not code:
        return None, "нет промокода"
    raw = code.strip().upper()
    if raw in ("5", "5%"):
        raw = "AUTO5"

    promo = PROMO_CODES.get(raw)
    if not promo:
        return None, "❌ Промокод не найден"

    exp = _parse_expire(promo.get("expires"))
    if exp and datetime.now() > exp:
        return None, "❌ Промокод истёк"

    limit = int(promo.get("limit") or 0)
    used = int(promo.get("used") or 0)
    if limit and used >= limit:
        return None, "❌ Лимит использований исчерпан"

    disc = int(promo.get("discount") or 0)
    if disc <= 0 or disc >= 100:
        return None, "❌ Некорректная скидка в промокоде"

    return disc, None

def use_promo(code: str):
    raw = (code or "").strip().upper()
    if raw in ("5", "5%"):
        raw = "AUTO5"
    if raw not in PROMO_CODES:
        return False
    PROMO_CODES[raw]["used"] = int(PROMO_CODES[raw].get("used") or 0) + 1
    save_json(PROMO_PATH, PROMO_CODES)
    return True

def calc_promo_discount(price: int, promo: str):
    """Считает цену со скидкой, НЕ тратит промокод."""
    disc, err = validate_promo(promo)
    if disc is None:
        return price, 0, err
    new_price = int(round(price * (100 - disc) / 100))
    return new_price, disc, None

def apply_promo(price: int, promo: str):
    """Считает цену со скидкой, НЕ тратит промокод (тратим в confirm)."""
    if not promo:
        return price, 0
    price2, pct, _err = calc_promo_discount(int(price), promo)
    return price2, pct

# =====================================================
# Keyboards
# =====================================================
def main_menu_keyboard(is_admin_user: bool = False):
    rows = [
        [KeyboardButton("🛒 Покупка"), KeyboardButton("ℹ️ Инфо")],
        [KeyboardButton("🆘 Поддержка"), KeyboardButton("📌 Статус заказа")],
    ]
    if is_admin_user:
        rows.append([KeyboardButton("🛠 Админ панель")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def buy_menu_keyboard():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📂 Каталог"), KeyboardButton("🎟 Промокод")],
            [KeyboardButton("⬅️ Назад")],
        ],
        resize_keyboard=True,
    )

def catalog_keyboard():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📚 СРС по гистологии (комплект)")],
            [KeyboardButton("🧠 Kahoot")],
            [KeyboardButton("📚 СРС (Самостоятельная работа)")],
            [KeyboardButton("📄 Реферат")],
            [KeyboardButton("📘 Доклад")],
            [KeyboardButton("📊 Презентация (PowerPoint)")],
            [KeyboardButton("⬅️ Назад")],
        ],
        resize_keyboard=True,
    )

def info_menu_keyboard():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("💰 Цена"), KeyboardButton("💳 Оплата")],
            [KeyboardButton("📦 Выдача"), KeyboardButton("⭐️ Оставить отзыв")],
            [KeyboardButton("⬅️ Назад")],
        ],
        resize_keyboard=True,
    )

def payment_keyboard_for_order(order_id: str):
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(f"💳 Я оплатил(а) №{order_id}")],
            [KeyboardButton("⬅️ В меню")],
        ],
        resize_keyboard=True,
    )

def review_keyboard():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("⭐️ Оставить отзыв")],
            [KeyboardButton("🏠 В меню")],
        ],
        resize_keyboard=True,
    )


def work_format_keyboard():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("✍️ От руки"), KeyboardButton("⌨️ Печатно")],
            [KeyboardButton("⬅️ Назад")],
        ],
        resize_keyboard=True,
    )

# =====================================================
# АДМИН-ПАНЕЛЬ
# =====================================================
def admin_panel_keyboard():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("🧾 Чеки (pending)")],
            [KeyboardButton("✅ Подтвердить"), KeyboardButton("❌ Отклонить")],
            [KeyboardButton("🟡 В работу"), KeyboardButton("🟢 Готово")],
            [KeyboardButton("📩 Выдано")],
            [KeyboardButton("💰 Выставить цену")],
            [KeyboardButton("💬 Ответ клиенту")],
            [KeyboardButton("➕ Добавить товар"), KeyboardButton("➖ Удалить товар")],
            [KeyboardButton("🎟➕ Добавить промокод"), KeyboardButton("🎟➖ Удалить промокод")],
            [KeyboardButton("📊 Статистика")],
            [KeyboardButton("📢 Рассылка")],
            [KeyboardButton("🚫 Забанить"), KeyboardButton("♻ Разбанить")],
            [KeyboardButton("🧹 Снять бан (спам)")],
            [KeyboardButton("⬅️ В меню")],
        ],
        resize_keyboard=True,
    )

# =====================================================
# Helpers
# =====================================================
def is_admin(update: Update) -> bool:
    return ADMIN_ID_INT is not None and update.effective_user and update.effective_user.id == ADMIN_ID_INT

def user_label(update: Update) -> str:
    u = update.effective_user
    if not u:
        return "unknown"
    uname = f"@{u.username}" if u.username else ""
    name = (u.full_name or "").strip()
    return f"{name} {uname}".strip()

def format_money(price):
    return f"{price} сом" if isinstance(price, int) else "цена по договорённости"

def extract_first_int(text: str):
    m = re.search(r"(\d{1,7})", text.replace(" ", ""))
    return int(m.group(1)) if m else None

def key_from_button_text(btn: str):
    if btn == "📚 СРС по гистологии (комплект)":
        return "gistology_ready"
    if btn == "🧠 Kahoot":
        return "kahoot"
    if btn == "📚 СРС (Самостоятельная работа)":
        return "srs"
    if btn == "📄 Реферат":
        return "referat"
    if btn == "📘 Доклад":
        return "doklad"
    if btn == "📊 Презентация (PowerPoint)":
        return "presentation"
    return None

def calc_suggested_price(product_key: str, volume_text: str):
    rule = PRICING_RULES.get(product_key)
    if not rule:
        return None, None
    unit, per_unit, minimum = rule
    qty = extract_first_int(volume_text)
    if not qty:
        return minimum, f"минимум {minimum}"
    price = max(minimum, qty * per_unit)
    return price, f"{qty} {unit}(ов) × {per_unit} сом (мин {minimum})"

def exit_support_mode(context: ContextTypes.DEFAULT_TYPE):
    """Выключает режим поддержки"""
    context.user_data["support_mode"] = False
    context.user_data["support_order_id"] = None

def order_status_human(status: str):
    mapping = {
        "needs_pricing": "⏳ Ожидает расчёта стоимости",
        "priced": "💳 Ожидает оплату",
        "reminded": "⏰ Напомнили об оплате",
        "pending": "🧾 Чек на проверке",
        "confirmed": "✅ Оплата подтверждена",
        "inwork": "🟡 В работе",
        "ready": "🟢 Готово",
        "delivered": "📩 Выдано/отправлено",
        "rejected": "❌ Отклонено",
        "support": "🆘 Поддержка",
    }
    return mapping.get(status, status)

def last_order_for_user(user_id: int):
    orders = ORDERS_DB.get("orders", {})
    items = [(oid, o) for oid, o in orders.items() if o.get("user_id") == user_id]
    items.sort(key=lambda x: int(x[0]))
    return items[-1] if items else (None, None)

# =====================================================
# Фоновая задача: напоминание об оплате
# =====================================================
async def unpaid_reminder(app):
    """Каждый час проверяет неоплаченные заказы и напоминает"""
    while True:
        try:
            now = datetime.now()
            orders_updated = False
            
            for oid, order in ORDERS_DB["orders"].items():
                if order.get("status") == "priced":
                    created = parse_iso(order.get("created_at"))
                    if created and (now - created).total_seconds() > 10800:  # 3 часа
                        user_id = order.get("user_id")
                        if user_id:
                            try:
                                await app.bot.send_message(
                                    user_id,
                                    f"⏰ Напоминание!\n\n"
                                    f"Заказ №{oid} ещё не оплачен.\n\n"
                                    f"🎁 Если оплатите сегодня — дам скидку 5%!\n"
                                    f"Напишите «поддержка» и укажите номер заказа."
                                )
                                order["status"] = "reminded"
                                order["updated_at"] = now_iso()
                                orders_updated = True
                            except Exception as e:
                                logging.error(f"Ошибка при отправке напоминания: {e}")
            
            if orders_updated:
                save_orders(ORDERS_DB)
                
        except Exception as e:
            logging.error(f"Ошибка в unpaid_reminder: {e}")
            
        await asyncio.sleep(3600)  # Проверка каждый час

# =====================================================
# Handlers: start + user
# =====================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Сохраняем пользователя в базу
    user_id = update.effective_user.id
    if user_id not in USERS_DB:
        USERS_DB.append(user_id)
        save_users(USERS_DB)
    
    # Сохраняем промокод, если был
    saved_promo = context.user_data.get("promo_default")
    context.user_data.clear()
    if saved_promo:
        context.user_data["promo_default"] = saved_promo
    
    await update.message.reply_text(
        "👋 Добро пожаловать в StubHub!\n\n"
        "📚 Здесь можно заказать работу или купить готовый комплект.\n\n"
        "Выберите действие 👇",
        reply_markup=main_menu_keyboard(is_admin(update)),
    )

async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    await update.message.reply_text(f"Ваш Telegram ID:\n{user.id}")

# =====================================================
# Admin panel command
# =====================================================
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    await update.message.reply_text(
        "🛠 Админ-панель:",
        reply_markup=admin_panel_keyboard(),
    )

# =====================================================
# Рассылка
# =====================================================
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    
    if not context.args:
        await update.message.reply_text("Использование: /broadcast Текст сообщения")
        return
    
    text = " ".join(context.args)
    users = USERS_DB
    success = 0
    failed = 0
    
    await update.message.reply_text(f"📢 Начинаю рассылку {len(users)} пользователям...")
    
    for uid in users:
        try:
            await context.bot.send_message(
                uid,
                f"📢 Уведомление от StubHub:\n\n{text}",
                reply_markup=main_menu_keyboard(False)
            )
            success += 1
            await asyncio.sleep(0.05)  # Небольшая задержка чтобы не флудить
        except Exception as e:
            failed += 1
            logging.error(f"Ошибка рассылки пользователю {uid}: {e}")
    
    await update.message.reply_text(
        f"✅ Рассылка завершена!\n"
        f"Успешно: {success}\n"
        f"Ошибок: {failed}"
    )

# =====================================================
# Form-based requirements
# =====================================================
FORM_QUESTIONS = [
    ("topic", "📝 Напишите тему."),
    ("volume", "📏 Укажите объём (страницы / слайды / количество вопросов)."),
    ("reqs", "📌 Требования (оформление/методичка/стиль). Если нет — напишите «нет»."),
    ("deadline", "⏰ Срок сдачи (дата/когда нужно)."),
    ("promo", "🎟 Если есть промокод — отправьте его. Если нет — напишите «нет»."),
]

def form_reset(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("form_step", None)
    context.user_data.pop("form_data", None)
    context.user_data.pop("selected_product", None)
    context.user_data.pop("work_format", None)
    context.user_data.pop("pending_format_product", None)

async def form_start(update: Update, context: ContextTypes.DEFAULT_TYPE, product_key: str):
    # Выходим из режима поддержки
    exit_support_mode(context)
    
    context.user_data["selected_product"] = product_key
    context.user_data["form_step"] = 0
    context.user_data["form_data"] = {}
    fmt = context.user_data.get("work_format")
    fmt_line = f"\n🖊 Формат: {fmt}" if fmt else ""
    await update.message.reply_text(
        f"✅ Вы выбрали: {PRODUCTS[product_key]['title']}" + fmt_line + "\n\n"
        "Заполним короткую форму (4–5 сообщений).",
        reply_markup=ReplyKeyboardRemove(),
    )
    await update.message.reply_text(FORM_QUESTIONS[0][1])

async def form_continue(update: Update, context: ContextTypes.DEFAULT_TYPE, user_text: str):
    step = context.user_data.get("form_step")
    if step is None:
        return False

    product_key = context.user_data.get("selected_product")
    form_data = context.user_data.get("form_data") or {}

    key, _question = FORM_QUESTIONS[step]
    form_data[key] = user_text.strip()
    context.user_data["form_data"] = form_data

    step += 1
    if step >= len(FORM_QUESTIONS):
        context.user_data["form_step"] = None

        topic = form_data.get("topic", "")
        volume = form_data.get("volume", "")
        reqs = form_data.get("reqs", "")
        deadline = form_data.get("deadline", "")
        promo = form_data.get("promo", "").strip()
        promo = "" if promo.lower() in ("нет", "no", "-") else promo.upper()

        urgent, urgent_fee, hours_left = urgency_fee_from_deadline(deadline)

        suggested_price, breakdown = calc_suggested_price(product_key, volume)
        if isinstance(suggested_price, int) and urgent_fee:
            suggested_price += urgent_fee
            breakdown = (breakdown or "").strip()
            extra = f"+{urgent_fee} сом (срочно)"
            breakdown = (breakdown + " | " + extra) if breakdown else extra

        if suggested_price is not None:
            sp2, pct = apply_promo(suggested_price, promo)
        else:
            sp2, pct = (None, 0)

        oid = new_order_id()
        u = update.effective_user
        product_title = PRODUCTS.get(product_key, {}).get("title", product_key)

        ORDERS_DB["orders"][str(oid)] = {
            "status": "needs_pricing",
            "user_id": u.id if u else None,
            "user_label": user_label(update),
            "product": product_key,
            "product_title": product_title,
            "details": {
                "topic": topic,
                "volume": volume,
                "reqs": reqs,
                "deadline": deadline,
                "format": context.user_data.get("work_format"),
            },
            "promo": promo if promo else None,
            "promo_pct": pct,
            "urgent": urgent,
            "urgent_fee": urgent_fee,
            "urgent_hours_left": hours_left,
            "suggested_price": sp2,
            "suggested_breakdown": breakdown,
            "created_at": now_iso(),
            "updated_at": now_iso(),
        }
        save_orders(ORDERS_DB)

        await update.message.reply_text(
            f"✅ Заявка принята! Номер: №{oid}\n\n"
            "Сейчас рассчитаю стоимость и напишу вам.\n"
            "Если нужно срочно — нажмите «🆘 Поддержка».",
            reply_markup=buy_menu_keyboard(),
        )

        if product_key == "presentation":
            await update.message.reply_text(
                "➕ Нужен ещё доклад к презентации?\n"
                "Могу сделать вместе со скидкой 10%. Напишите: «да» или «нет».",
                reply_markup=ReplyKeyboardRemove(),
            )
            context.user_data["upsell_for_order"] = str(oid)

        if ADMIN_ID_INT is not None:
            promo_line = f"\nПромокод: {promo} (-{pct}%)" if promo and pct else (f"\nПромокод: {promo} (не найден)" if promo else "")
            sug_line = f"\n💡 Автоцена: {sp2} сом ({breakdown}){promo_line}" if sp2 is not None else ""

            await context.bot.send_message(
                chat_id=ADMIN_ID_INT,
                text=(
                    f"🆕 Новая заявка №{oid}\n"
                    f"Клиент: {user_label(update)}\n"
                    f"User ID: {u.id if u else 'unknown'}\n"
                    f"Услуга: {product_title}\n"
                    f"{sug_line}\n\n"
                    f"Требования:\n"
                    f"• Тема: {topic}\n"
                    f"• Объём: {volume}\n"
                    f"• Формат: {context.user_data.get('work_format') or '—'}\n"
                    f"• Требования: {reqs}\n"
                    f"• Срок: {deadline}\n\n"
                    f"Выставить цену: /setprice {oid} 700"
                ),
            )
        form_reset(context)
        return True

    context.user_data["form_step"] = step
    await update.message.reply_text(FORM_QUESTIONS[step][1])
    return True

# =====================================================
# User main message handler
# =====================================================
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_text = (update.message.text or "").strip()
    if not user_text:
        return
    lower_text = user_text.lower()
    is_admin_user = is_admin(update)

    # --- Проверка бана ---
    user_id = update.effective_user.id
    now = datetime.now()

    ban_data = BANS.get(str(user_id))

    if ban_data and (not is_admin_user):
        if ban_data["type"] == "perm":
            reason = ban_data.get("reason", "без причины")
            await update.message.reply_text(
                f"🚫 Вы навсегда заблокированы.\n"
                f"Причина: {reason}\n\n"
                f"Если считаете это ошибкой — @slt_nv"
            )
            return

        elif ban_data["type"] == "temp":
            until = datetime.fromisoformat(ban_data["until"])
            if now < until:
                remaining = int((until - now).total_seconds() / 60)
                await update.message.reply_text(
                    f"🚫 Вы заблокированы за спам.\n"
                    f"Попробуйте через {remaining} минут.\n\n"
                    f"Если ошибка — @slt_nv"
                )
                return
            else:
                del BANS[str(user_id)]
                save_bans(BANS)

    # --- Анти-спам с прогрессивным баном ---
    if not is_admin_user:
        history = SPAM_TRACKER.get(user_id, [])
        history = [t for t in history if (now - t).total_seconds() < SPAM_SECONDS]
        history.append(now)
        SPAM_TRACKER[user_id] = history
        
        if len(history) > SPAM_LIMIT:
            strikes = BANS.get(str(user_id), {}).get("strikes", 0) + 1
        
            if strikes > len(BAN_STEPS):
                # PERM BAN
                BANS[str(user_id)] = {
                    "type": "perm",
                    "reason": "спам",
                    "strikes": strikes
                }
                save_bans(BANS)
        
                await update.message.reply_text(
                    "🚫 Вы навсегда заблокированы.\n"
                    "Причина: спам\n\n"
                    "Если ошибка — @slt_nv"
                )
        
                # уведомление админу
                if ADMIN_ID_INT:
                    await context.bot.send_message(
                        ADMIN_ID_INT,
                        f"🚫 PERM BAN\nUser ID: {user_id}\nПричина: спам"
                    )
                return
        
            minutes = BAN_STEPS[strikes - 1]
        
            BANS[str(user_id)] = {
                "type": "temp",
                "until": (now + timedelta(minutes=minutes)).isoformat(),
                "reason": "спам",
                "strikes": strikes
            }
            save_bans(BANS)
        
            await update.message.reply_text(
                f"🚫 Вы заблокированы за спам.\n"
                f"Срок: {minutes} минут.\n\n"
                f"Если ошибка — @slt_nv"
            )
            return
        
        
# ================= ОТПРАВКА ФАЙЛА ПОКУПАТЕЛЮ =================
    if is_admin(update) and context.user_data.get("send_file_order"):
        oid = context.user_data.get("send_file_order")
        order = ORDERS_DB.get("orders", {}).get(oid)

        if not order:
            await update.message.reply_text("❌ Заказ не найден.")
            context.user_data["send_file_order"] = None
            return

        user_id = order.get("user_id")

        if not user_id:
            await update.message.reply_text("❌ У заказа нет user_id.")
            context.user_data["send_file_order"] = None
            return

        # Если админ отправил текст (ссылку)
        if update.message.text:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"📩 Ваш заказ №{oid} готов:\n\n{update.message.text}\n\n⭐️ Будем рады отзыву 🙏",
                reply_markup=review_keyboard(),
            )

        # Если админ отправил документ
        elif update.message.document:
            await context.bot.send_document(
                chat_id=user_id,
                document=update.message.document.file_id,
                caption=f"📩 Ваш заказ №{oid} готов!\n\n⭐️ Будем рады отзыву 🙏",
                reply_markup=review_keyboard(),
            )

        # Если админ отправил фото
        elif update.message.photo:
            await context.bot.send_photo(
                chat_id=user_id,
                photo=update.message.photo[-1].file_id,
                caption=f"📩 Ваш заказ №{oid} готов!\n\n⭐️ Будем рады отзыву 🙏",
                reply_markup=review_keyboard(),
            )

        # меняем статус
        order["status"] = "delivered"
        order["updated_at"] = now_iso()
        save_orders(ORDERS_DB)

        await update.message.reply_text("✅ Материал отправлен клиенту.", reply_markup=admin_panel_keyboard())

        context.user_data["send_file_order"] = None
        return

    # ================= ОБРАБОТКА ОТЗЫВА =================
    if context.user_data.get("waiting_review"):
        context.user_data["waiting_review"] = False

        review_text = user_text
        user = update.effective_user

        await update.message.reply_text(
            "🙏 Спасибо за отзыв!\n\nМы ценим ваше мнение 💎",
            reply_markup=main_menu_keyboard(is_admin_user),
        )

        # Отправляем админу
        if ADMIN_ID_INT:
            await context.bot.send_message(
                ADMIN_ID_INT,
                f"⭐ Новый отзыв\n\n"
                f"От: {user.full_name}\n"
                f"Username: @{user.username if user.username else 'нет'}\n"
                f"ID: {user.id}\n\n"
                f"{review_text}"
            )
        return

    # ================= ADMIN PANEL =================
    if is_admin(update):
        # Кнопки админ-панели
        if user_text == "🧾 Чеки (pending)":
            await pending(update, context)
            return

        if user_text == "📊 Статистика":
            await stats(update, context)
            return

        if user_text == "✅ Подтвердить":
            context.user_data["admin_action"] = "confirm"
            await update.message.reply_text("Введите номер заказа:")
            return

        if user_text == "❌ Отклонить":
            context.user_data["admin_action"] = "reject"
            await update.message.reply_text("Введите номер заказа:")
            return

        if user_text == "🟡 В работу":
            context.user_data["admin_action"] = "inwork"
            await update.message.reply_text("Введите номер заказа:")
            return

        if user_text == "🟢 Готово":
            context.user_data["admin_action"] = "ready"
            await update.message.reply_text("Введите номер заказа:")
            return

        if user_text == "📩 Выдано":
            context.user_data["admin_action"] = "send_file"
            await update.message.reply_text("Введите номер заказа:")
            return

        if user_text == "💰 Выставить цену":
            context.user_data["admin_action"] = "setprice"
            await update.message.reply_text("Введите номер и сумму. Пример: 25 700")
            return

        if user_text == "💬 Ответ клиенту":
            context.user_data["admin_action"] = "reply"
            await update.message.reply_text("Введите номер и текст. Пример: 25 Готово")
            return

        if user_text == "📢 Рассылка":
            context.user_data["admin_action"] = "broadcast"
            await update.message.reply_text("Введите текст для рассылки:")
            return

        if user_text == "🚫 Забанить":
            context.user_data["admin_action"] = "ban"
            await update.message.reply_text("Введите USER_ID и причину:\nПример: 123456 грубость")
            return

        if user_text == "♻ Разбанить":
            context.user_data["admin_action"] = "unban"
            await update.message.reply_text("Введите USER_ID:")
            return

        if user_text == "🧹 Снять бан (спам)":
            context.user_data["admin_action"] = "unban_spam"
            await update.message.reply_text("Введите USER_ID для снятия спам-бана:")
            return
            
        if user_text == "➕ Добавить товар":
            context.user_data["admin_action"] = "add_product"
            await update.message.reply_text(
                "Формат:\nkey|Название|type(ready/individual)|price(если ready)|delivery_doc(если ready)\n"
                "Пример:\nnewpack|📦 Новый комплект|ready|399|delivery_newpack.txt"
            )
            return

        if user_text == "➖ Удалить товар":
            context.user_data["admin_action"] = "del_product"
            await update.message.reply_text("Введите key товара (например: kahoot):")
            return

        if user_text == "🎟➕ Добавить промокод":
            context.user_data["admin_action"] = "add_promo"
            await update.message.reply_text(
                "Формат:\nCODE|DISCOUNT|YYYY-MM-DD(или пусто)|LIMIT\n"
                "Пример:\nSPRING10|10|2026-04-01|100"
            )
            return

        if user_text == "🎟➖ Удалить промокод":
            context.user_data["admin_action"] = "del_promo"
            await update.message.reply_text("Введите CODE промокода:")
            return


        # обработка ввода после кнопки
        action = context.user_data.get("admin_action")
        if action:
            # Если есть активный заказ, используем его
            oid = context.user_data.get("active_order")

            if action in ("confirm", "reject", "inwork", "ready", "send_file", "setprice", "reply"):
                if not oid:
                    oid = extract_first_int(user_text)
                    if not oid:
                        await update.message.reply_text("Введите корректный номер.")
                        return
                    context.user_data["active_order"] = str(oid)

                if action in ("confirm", "reject", "inwork", "ready"):
                    context.args = [str(oid)]

                    if action == "confirm":
                        await confirm(update, context)
                    elif action == "reject":
                        await reject(update, context)
                    elif action == "inwork":
                        await inwork(update, context)
                    elif action == "ready":
                        await ready(update, context)

                    context.user_data["admin_action"] = None
                    await update.message.reply_text("✅ Готово.", reply_markup=admin_panel_keyboard())
                    return

                if action == "send_file":
                    order = ORDERS_DB.get("orders", {}).get(str(oid))
                    if not order:
                        await update.message.reply_text("Заказ не найден.")
                        context.user_data["admin_action"] = None
                        return

                    context.user_data["send_file_order"] = str(oid)
                    context.user_data["admin_action"] = None

                    await update.message.reply_text(
                        f"📎 Теперь отправьте файл или ссылку для заказа №{oid}."
                    )
                    return

                if action == "setprice":
                    parts = user_text.split()
                    if len(parts) < 2:
                        await update.message.reply_text("Формат: 25 700")
                        return
                    context.args = parts
                    await setprice(update, context)
                    context.user_data["admin_action"] = None
                    await update.message.reply_text("✅ Готово.", reply_markup=admin_panel_keyboard())
                    return

                if action == "reply":
                    parts = user_text.split(maxsplit=1)
                    if len(parts) < 2:
                        await update.message.reply_text("Формат: 25 Текст")
                        return
                    context.args = parts
                    await reply(update, context)
                    context.user_data["admin_action"] = None
                    await update.message.reply_text("✅ Готово.", reply_markup=admin_panel_keyboard())
                    return

            if action == "ban":
                parts = user_text.split(maxsplit=1)
                if len(parts) < 2:
                    await update.message.reply_text("Формат: USER_ID причина")
                    return

                uid = parts[0].strip()
                reason = parts[1].strip()

                BANS[str(uid)] = {
                    "type": "perm",
                    "reason": reason,
                    "strikes": 999
                }
                save_bans(BANS)

                try:
                    await context.bot.send_message(
                        chat_id=int(uid),
                        text=f"🚫 Вы навсегда заблокированы администратором.\n"
                             f"Причина: {reason}\n\n"
                             f"Если ошибка — @slt_nv"
                    )
                except:
                    pass

                await update.message.reply_text("✅ Пользователь заблокирован.")
                context.user_data["admin_action"] = None
                return

            if action == "unban":
                uid = user_text.strip()
                if uid in BANS:
                    del BANS[uid]
                    save_bans(BANS)

                await update.message.reply_text("✅ Пользователь разблокирован.")
                context.user_data["admin_action"] = None
                return

            if action == "unban_spam":
                uid = user_text.strip()

                if uid in BANS:
                    ban_data = BANS.get(uid)

                    # снимаем только если это спам
                    if ban_data.get("reason") == "спам":
                        del BANS[uid]
                        save_bans(BANS)

                        await update.message.reply_text("✅ Спам-бан снят.")
                    else:
                        await update.message.reply_text("⚠ Это не спам-бан.")
                else:
                    await update.message.reply_text("Пользователь не забанен.")

                context.user_data["admin_action"] = None
                return

            
            if action == "add_product":
                parts = [p.strip() for p in user_text.split("|")]
                if len(parts) < 3:
                    await update.message.reply_text("Формат неверный. Попробуйте снова.")
                    return
                key = parts[0]
                title = parts[1]
                ptype = parts[2].lower()
                if ptype not in ("ready", "individual"):
                    await update.message.reply_text("type должен быть ready или individual.")
                    return
                prod = {"title": title, "type": ptype}
                if ptype == "ready":
                    price = extract_first_int(parts[3] if len(parts) > 3 else "") or 0
                    delivery_doc = parts[4] if len(parts) > 4 else ""
                    prod["price"] = int(price)
                    if delivery_doc:
                        prod["delivery_doc"] = delivery_doc
                PRODUCTS[key] = prod
                save_json(PRODUCTS_PATH, PRODUCTS)
                await update.message.reply_text("✅ Товар добавлен.", reply_markup=admin_panel_keyboard())
                context.user_data["admin_action"] = None
                return

            if action == "del_product":
                key = user_text.strip()
                if key in PRODUCTS:
                    PRODUCTS.pop(key, None)
                    save_json(PRODUCTS_PATH, PRODUCTS)
                    await update.message.reply_text("✅ Товар удалён.", reply_markup=admin_panel_keyboard())
                else:
                    await update.message.reply_text("❌ Такой key не найден.", reply_markup=admin_panel_keyboard())
                context.user_data["admin_action"] = None
                return

            if action == "add_promo":
                parts = [p.strip() for p in user_text.split("|")]
                if len(parts) < 2:
                    await update.message.reply_text("Формат: CODE|DISCOUNT|YYYY-MM-DD|LIMIT")
                    return
                code = parts[0].upper()
                disc = extract_first_int(parts[1])
                if disc is None or disc <= 0 or disc >= 100:
                    await update.message.reply_text("DISCOUNT должен быть числом 1..99")
                    return
                exp = parts[2] if len(parts) > 2 and parts[2] else None
                lim = extract_first_int(parts[3]) if len(parts) > 3 else 10**9
                PROMO_CODES[code] = {"discount": int(disc), "expires": exp, "limit": int(lim or 0), "used": 0}
                save_json(PROMO_PATH, PROMO_CODES)
                await update.message.reply_text("✅ Промокод добавлен.", reply_markup=admin_panel_keyboard())
                context.user_data["admin_action"] = None
                return

            if action == "del_promo":
                code = user_text.strip().upper()
                if code in ("AUTO5",):
                    await update.message.reply_text("⚠️ AUTO5 встроенный, нельзя удалить.", reply_markup=admin_panel_keyboard())
                elif code in PROMO_CODES:
                    PROMO_CODES.pop(code, None)
                    save_json(PROMO_PATH, PROMO_CODES)
                    await update.message.reply_text("✅ Промокод удалён.", reply_markup=admin_panel_keyboard())
                else:
                    await update.message.reply_text("❌ Не найден.", reply_markup=admin_panel_keyboard())
                context.user_data["admin_action"] = None
                return

            if action == "broadcast":
                context.args = [user_text]
                await broadcast(update, context)
                context.user_data["admin_action"] = None
                await update.message.reply_text("✅ Готово.", reply_markup=admin_panel_keyboard())
                return

    # Если идёт форма — обрабатываем в приоритете
    if context.user_data.get("form_step") is not None:
        handled = await form_continue(update, context, user_text)
        if handled:
            return

    # Upsell answer
    if context.user_data.get("upsell_for_order"):
        if lower_text in ("да", "yes", "+"):
            oid = context.user_data.pop("upsell_for_order")
            order = ORDERS_DB.get("orders", {}).get(str(oid))
            if order:
                order["upsell"] = "doklad_with_discount_10"
                order["updated_at"] = now_iso()
                save_orders(ORDERS_DB)
                await update.message.reply_text("✅ Добавил к заявке: доклад (-10%).", reply_markup=buy_menu_keyboard())
                if ADMIN_ID_INT is not None:
                    await context.bot.send_message(
                        ADMIN_ID_INT,
                        f"➕ Upsell: к №{oid} добавили доклад (-10%).",
                    )
            else:
                await update.message.reply_text("Ок.", reply_markup=buy_menu_keyboard())
            return
        if lower_text in ("нет", "no", "-"):
            context.user_data.pop("upsell_for_order")
            await update.message.reply_text("Ок 🙂", reply_markup=buy_menu_keyboard())
            return
        context.user_data.pop("upsell_for_order")

    # Навигация
    if user_text == "🛠 Админ панель" and is_admin_user:
        await admin_panel(update, context)
        return

    if user_text == "🏠 В меню":
        await update.message.reply_text("🏠 Главное меню:", reply_markup=main_menu_keyboard(is_admin_user))
        return

    if user_text == "⬅️ Назад":
        await update.message.reply_text("🏠 Главное меню:", reply_markup=main_menu_keyboard(is_admin_user))
        return

    if user_text == "⬅️ В меню":
        await update.message.reply_text("🛒 Раздел покупок:", reply_markup=buy_menu_keyboard())
        return

    if user_text == "🛒 Покупка":
        # 🔥 отключаем поддержку если была включена
        exit_support_mode(context)
        await update.message.reply_text("🛒 Раздел покупок:", reply_markup=buy_menu_keyboard())
        return

    if user_text == "📂 Каталог":
        # 🔥 отключаем поддержку если была включена
        exit_support_mode(context)
        await update.message.reply_text("📦 Выберите товар/услугу:", reply_markup=catalog_keyboard())
        return

    if user_text == "ℹ️ Инфо":
        await update.message.reply_text("ℹ️ Информация:", reply_markup=info_menu_keyboard())
        return

    # Промокод
    if user_text == "🎟 Промокод":
        await update.message.reply_text(
            "Отправьте промокод одним сообщением.\nНапример: PROMO10\n\n"
            "Если нет — напишите «нет».",
            reply_markup=ReplyKeyboardRemove(),
        )
        context.user_data["waiting_promo_only"] = True
        return

    if context.user_data.get("waiting_promo_only"):
        context.user_data["waiting_promo_only"] = False
        promo = user_text.strip()
        if promo.lower() in ("нет", "no", "-"):
            context.user_data["promo_default"] = ""
            await update.message.reply_text("Ок, без промокода 🙂", reply_markup=buy_menu_keyboard())
        else:
            promo_u = promo.upper()
            discount, error = validate_promo(promo_u)
            if discount:
                context.user_data["promo_default"] = promo_u
                await update.message.reply_text(
                    f"✅ Промокод {promo_u} активирован (-{discount}%)!\n"
                    f"Действует на текущий заказ.",
                    reply_markup=buy_menu_keyboard()
                )
            else:
                context.user_data["promo_default"] = promo_u
                await update.message.reply_text(
                    f"⚠️ {error}\n"
                    f"Но я сохраню — менеджер проверит.",
                    reply_markup=buy_menu_keyboard()
                )
        return

    # Инфо раздел
    if user_text == "💰 Цена":
        txt = get_doc_by_name("prices.txt")
        await update.message.reply_text(txt or "Добавь knowledge/prices.txt → python index_kb.py")
        return

    if user_text == "💳 Оплата":
        await update.message.reply_text(get_payment_text())
        return

    if user_text == "📦 Выдача":
        txt = get_doc_by_name("delivery.txt")
        await update.message.reply_text(txt or "Добавь knowledge/delivery.txt → python index_kb.py")
        return

    # Отзыв
    if user_text == "⭐️ Оставить отзыв":
        context.user_data["waiting_review"] = True
        await update.message.reply_text(
            "✍️ Напишите ваш отзыв одним сообщением:",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    # Статус заказа
    if user_text == "📌 Статус заказа":
        oid, order = last_order_for_user(update.effective_user.id)
        if not order:
            await update.message.reply_text("Пока нет заказов. Откройте «📂 Каталог».", reply_markup=buy_menu_keyboard())
            return
        st = order_status_human(order.get("status"))
        title = order.get("product_title") or order.get("product")
        price = order.get("price")
        await update.message.reply_text(
            f"📌 Ваш последний заказ:\n"
            f"№{oid}\n"
            f"Услуга: {title}\n"
            f"Статус: {st}\n"
            f"Сумма: {format_money(price)}",
            reply_markup=main_menu_keyboard(is_admin_user),
        )
        return

    # Поддержка
    if user_text == "🆘 Поддержка" or "менеджер" in lower_text:
        oid = new_order_id()
        u = update.effective_user
        ORDERS_DB["orders"][str(oid)] = {
            "status": "support",
            "user_id": u.id if u else None,
            "user_label": user_label(update),
            "product": context.user_data.get("selected_product"),
            "price": None,
            "details": "support_ticket",
            "created_at": now_iso(),
            "updated_at": now_iso(),
        }
        save_orders(ORDERS_DB)

        context.user_data["support_mode"] = True
        context.user_data["support_order_id"] = str(oid)

        txt = get_doc_by_name("support.txt") or "Опишите проблему одним сообщением — я передам менеджеру."
        await update.message.reply_text(
            f"{txt}\n\n📌 Номер обращения: №{oid}\n\nЧтобы закрыть поддержку нажмите: «❌ Закрыть поддержку»",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton("❌ Закрыть поддержку")]], resize_keyboard=True),
        )

        if ADMIN_ID_INT is not None:
            await context.bot.send_message(
                chat_id=ADMIN_ID_INT,
                text=(
                    f"🆘 Новое обращение №{oid}\n"
                    f"Клиент: {user_label(update)}\n"
                    f"User ID: {u.id if u else 'unknown'}\n\n"
                    f"Ответить: /reply {oid} текст"
                ),
            )
        return

    if user_text == "❌ Закрыть поддержку":
        exit_support_mode(context)
        await update.message.reply_text("✅ Поддержка закрыта. Возвращаю меню.", reply_markup=main_menu_keyboard(is_admin_user))
        return

    if context.user_data.get("support_mode") and ADMIN_ID_INT is not None:
        oid = context.user_data.get("support_order_id", "?")
        u = update.effective_user
        await context.bot.send_message(
            chat_id=ADMIN_ID_INT,
            text=(
                f"💬 Сообщение клиента (обращение №{oid})\n"
                f"Клиент: {user_label(update)}\n"
                f"User ID: {u.id if u else 'unknown'}\n\n"
                f"{user_text}\n\n"
                f"Ответить: /reply {oid} текст"
            ),
        )
        await update.message.reply_text("✅ Передал менеджеру. Ожидайте ответ.")
        return

    # Выбор формата (для СРС/Доклад/Реферат)
    if context.user_data.get("pending_format_product"):
        pk = context.user_data.get("pending_format_product")
        if user_text in ("✍️ От руки", "⌨️ Печатно"):
            context.user_data["work_format"] = "От руки" if user_text == "✍️ От руки" else "Печатно"
            context.user_data["pending_format_product"] = None
            await form_start(update, context, pk)
            promo_default = context.user_data.get("promo_default", "")
            if promo_default:
                await update.message.reply_text(f"🎟 У вас активен промокод: {promo_default} (учту в цене).")
            return

    # Каталог
    product_key = key_from_button_text(user_text)
    if product_key:
        # 🔥 отключаем поддержку если была включена
        exit_support_mode(context)
        
        promo_default = context.user_data.get("promo_default", "")
        if product_key == "gistology_ready":
            oid = new_order_id()
            u = update.effective_user
            product = PRODUCTS[product_key]
            price = int(product.get("price", 0))

            price2, pct = apply_promo(price, promo_default) if price else (price, 0)

            ORDERS_DB["orders"][str(oid)] = {
                "status": "priced",
                "user_id": u.id if u else None,
                "user_label": user_label(update),
                "product": product_key,
                "product_title": product["title"],
                "price": price2,
                "promo": promo_default,
                "promo_pct": pct,
                "details": "ready_product",
                "created_at": now_iso(),
                "updated_at": now_iso(),
            }
            save_orders(ORDERS_DB)

            pay = get_payment_text()
            promo_line = f"\n🎟 Промокод: {promo_default} (-{pct}%)" if promo_default and pct else ""
            await update.message.reply_text(
                (
                    f"✅ Вы выбрали: {product['title']}\n"
                    f"№{oid}\n"
                    f"💰 К оплате: {price2} сом{promo_line}\n\n"
                    f"{pay}\n\n"
                    f"После оплаты нажмите «💳 Я оплатил(а) №{oid}» и отправьте чек."
                ),
                reply_markup=payment_keyboard_for_order(str(oid)),
            )
            return

        # Для СРС/Реферат/Доклад сначала спрашиваем формат (от руки / печатно)
        if product_key in ("srs", "referat", "doklad"):
            context.user_data["pending_format_product"] = product_key
            await update.message.reply_text(
                "✍️ Пожалуйста, выберите формат кнопкой ниже:\n"
                "• От руки\n"
                "• Печатно",
                reply_markup=work_format_keyboard(),
            )
            return

        await form_start(update, context, product_key)
        if promo_default:
            await update.message.reply_text(f"🎟 У вас активен промокод: {promo_default} (учту в цене).")
        return

    # Кнопка оплаты
    m = re.match(r"^💳\s*Я оплатил\(а\)\s*№(\d+)\s*$", user_text)
    if m:
        oid = m.group(1)
        order = ORDERS_DB.get("orders", {}).get(oid)
        if not order or order.get("user_id") != update.effective_user.id:
            await update.message.reply_text("❌ Заказ не найден. Откройте «📂 Каталог».", reply_markup=buy_menu_keyboard())
            return
        if order.get("status") != "priced":
            await update.message.reply_text(
                f"По заказу №{oid} сейчас статус: {order_status_human(order.get('status'))}.\n"
                "Если нужен менеджер — нажмите «🆘 Поддержка».",
                reply_markup=main_menu_keyboard(is_admin_user),
            )
            return

        context.user_data["awaiting_receipt_order_id"] = oid
        await update.message.reply_text(
            f"🧾 Отлично! Отправьте чек (фото) по заказу №{oid}.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    # Default
    await update.message.reply_text(
        "Не понял 🤝\n"
        "Откройте «🛒 Покупка» → «📂 Каталог».\n"
        "Или нажмите «🆘 Поддержка».",
        reply_markup=main_menu_keyboard(is_admin_user),
    )

# =====================================================
# Receipt photo handler
# =====================================================
async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    is_admin_user = is_admin(update)
    oid = context.user_data.get("awaiting_receipt_order_id")
    if not oid:
        await update.message.reply_text(
            "📷 Фото получено.\nЕсли это чек — нажмите кнопку «💳 Я оплатил(а) №...».",
            reply_markup=main_menu_keyboard(is_admin_user),
        )
        return

    context.user_data["awaiting_receipt_order_id"] = None

    order = ORDERS_DB.get("orders", {}).get(str(oid))
    if not order:
        await update.message.reply_text("❌ Заказ не найден.", reply_markup=main_menu_keyboard(is_admin_user))
        return

    if order.get("user_id") != update.effective_user.id:
        await update.message.reply_text("❌ Это не ваш заказ.", reply_markup=main_menu_keyboard(is_admin_user))
        context.user_data["awaiting_receipt_order_id"] = None
        return

    if order.get("status") != "priced":
        await update.message.reply_text(
            f"По заказу №{oid} сейчас статус: {order_status_human(order.get('status'))}.",
            reply_markup=main_menu_keyboard(is_admin_user),
        )
        return

    order["status"] = "pending"
    order["updated_at"] = now_iso()
    save_orders(ORDERS_DB)

    await update.message.reply_text("✅ Чек получен и отправлен на проверку.", reply_markup=main_menu_keyboard(is_admin_user))

    if ADMIN_ID_INT is None:
        await update.message.reply_text(
            "⚠️ ADMIN_ID не задан в .env, поэтому чек не может уйти админу.\n"
            "Добавьте ADMIN_ID и перезапустите бота."
        )
        return

    try:
        await update.message.forward(chat_id=ADMIN_ID_INT)
    except Exception:
        pass

    await context.bot.send_message(
        chat_id=ADMIN_ID_INT,
        text=(
            f"🧾 Новый чек\n"
            f"№{oid}\n"
            f"Клиент: {order.get('user_label')}\n"
            f"User ID: {order.get('user_id')}\n"
            f"Товар/услуга: {order.get('product_title')}\n"
            f"Сумма: {format_money(order.get('price'))}\n\n"
            f"Подтвердить: /confirm {oid}\n"
            f"Отклонить: /reject {oid}"
        ),
    )

# =====================================================
# Admin commands
# =====================================================
async def msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if len(context.args) < 2:
        await update.message.reply_text("Использование: /msg USER_ID текст")
        return
    try:
        user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("USER_ID должен быть числом.")
        return
    text = " ".join(context.args[1:]).strip()
    if not text:
        await update.message.reply_text("Текст пустой.")
        return
    try:
        await context.bot.send_message(chat_id=user_id, text=text, reply_markup=main_menu_keyboard(False))
        await update.message.reply_text("✅ Отправлено клиенту.")
    except Exception as e:
        await update.message.reply_text(f"⚠️ Не удалось отправить: {e}")

async def reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if len(context.args) < 2:
        await update.message.reply_text("Использование: /reply НОМЕР текст")
        return
    oid = context.args[0].strip()
    text = " ".join(context.args[1:]).strip()
    order = ORDERS_DB.get("orders", {}).get(oid)
    if not order:
        await update.message.reply_text(f"Заказ/обращение №{oid} не найден.")
        return
    user_id = order.get("user_id")
    if not user_id:
        await update.message.reply_text("⚠️ У заказа нет user_id.")
        return
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=f"👨‍💼 Менеджер:\n{text}",
            reply_markup=main_menu_keyboard(False),
        )
        await update.message.reply_text("✅ Отправлено.")
    except Exception as e:
        await update.message.reply_text(f"⚠️ Не удалось отправить: {e}")

async def setprice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not context.args:
        await update.message.reply_text("Использование: /setprice НОМЕР СУММА")
        return

    oid = context.args[0].strip()
    raw = " ".join(context.args[1:]).strip()
    price = extract_first_int(raw)
    if price is None:
        await update.message.reply_text("Сумма должна содержать число (например 700).")
        return

    order = ORDERS_DB.get("orders", {}).get(oid)
    if not order:
        await update.message.reply_text(f"Заказ №{oid} не найден.")
        return
        
    if order.get("status") not in ("needs_pricing", "priced", "pending"):
        await update.message.reply_text(
            f"Заказ №{oid} уже в статусе {order_status_human(order.get('status'))}. "
            "Изменить цену можно только для новых заказов."
        )
        return

    user_id = order.get("user_id")
    if not user_id:
        await update.message.reply_text("⚠️ У заказа нет user_id.")
        return

    promo = (order.get("promo") or "").upper()
    price2, pct = apply_promo(int(price), promo) if price else (int(price), 0)

    urgent_fee = int(order.get("urgent_fee") or 0)
    if urgent_fee:
        price2 = int(price2) + urgent_fee

    order["price"] = int(price2)
    order["promo_pct"] = pct
    order["status"] = "priced"
    order["updated_at"] = now_iso()
    save_orders(ORDERS_DB)

    pay = get_payment_text()
    promo_line = f"\n🎟 Промокод: {promo} (-{pct}%)" if promo and pct else ""

    await context.bot.send_message(
        chat_id=user_id,
        text=(
            f"✅ Стоимость рассчитана.\n\n"
            f"№{oid}\n"
            f"💰 К оплате: {price2} сом{promo_line}\n\n"
            f"{pay}\n\n"
            f"После оплаты нажмите «💳 Я оплатил(а) №{oid}» и отправьте чек."
        ),
        reply_markup=payment_keyboard_for_order(oid),
    )
    await update.message.reply_text(f"✅ Цена отправлена клиенту (№{oid}).")

async def inwork(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): 
        return
    if not context.args:
        await update.message.reply_text("Использование: /inwork НОМЕР")
        return
    oid = context.args[0].strip()
    order = ORDERS_DB.get("orders", {}).get(oid)
    if not order:
        await update.message.reply_text("Не найден.")
        return
    order["status"] = "inwork"
    order["updated_at"] = now_iso()
    save_orders(ORDERS_DB)
    uid = order.get("user_id")
    if uid:
        await context.bot.send_message(uid, f"🟡 Заказ №{oid} взят в работу.\nМы напишем, когда будет готово.", reply_markup=main_menu_keyboard(False))
    await update.message.reply_text("✅ Статус: в работе.")

async def ready(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): 
        return
    if not context.args:
        await update.message.reply_text("Использование: /ready НОМЕР")
        return
    oid = context.args[0].strip()
    order = ORDERS_DB.get("orders", {}).get(oid)
    if not order:
        await update.message.reply_text("Не найден.")
        return
    order["status"] = "ready"
    order["updated_at"] = now_iso()
    save_orders(ORDERS_DB)
    uid = order.get("user_id")
    if uid:
        await context.bot.send_message(uid, f"🟢 Заказ №{oid} готов!\nНапишите «поддержка», если нужно что-то уточнить.", reply_markup=main_menu_keyboard(False))
    await update.message.reply_text("✅ Статус: готово.")

async def delivered(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): 
        return
    if not context.args:
        await update.message.reply_text("Использование: /delivered НОМЕР")
        return
    oid = context.args[0].strip()
    order = ORDERS_DB.get("orders", {}).get(oid)
    if not order:
        await update.message.reply_text("Не найден.")
        return
    order["status"] = "delivered"
    order["updated_at"] = now_iso()
    save_orders(ORDERS_DB)
    uid = order.get("user_id")
    if uid:
        await context.bot.send_message(
            uid,
            f"📩 Заказ №{oid} отправлен/выдан.\n\n⭐️ Будем рады отзыву 🙏",
            reply_markup=review_keyboard(),
        )
    await update.message.reply_text("✅ Статус: выдано.")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update): 
        return
    period = (context.args[0].lower() if context.args else "day")
    now = datetime.now()
    if period == "week":
        start = now - timedelta(days=7)
        label = "за 7 дней"
    elif period == "month":
        start = now - timedelta(days=30)
        label = "за 30 дней"
    else:
        start = now - timedelta(days=1)
        label = "за 24 часа"

    orders = ORDERS_DB.get("orders", {})
    count = 0
    paid = 0
    paid_sum = 0
    by_product = {}

    for oid, o in orders.items():
        dt = parse_iso(o.get("created_at", ""))
        if not dt or dt < start:
            continue
        count += 1
        status = o.get("status")
        if status in ("confirmed", "inwork", "ready", "delivered"):
            paid += 1
        price = o.get("price")
        if status in ("confirmed", "inwork", "ready", "delivered") and isinstance(price, int):
            paid_sum += price
        ptitle = o.get("product_title") or o.get("product") or "unknown"
        by_product[ptitle] = by_product.get(ptitle, 0) + 1

    top = sorted(by_product.items(), key=lambda x: x[1], reverse=True)[:5]
    top_lines = "\n".join([f"• {name}: {cnt}" for name, cnt in top]) if top else "—"

    avg = int(round(paid_sum / paid)) if paid else 0

    await update.message.reply_text(
        f"📊 Статистика {label}\n\n"
        f"Заявок: {count}\n"
        f"Оплачено (подтв.): {paid}\n"
        f"Сумма оплат: {paid_sum} сом\n"
        f"Средний чек: {avg} сом\n\n"
        f"Топ услуг:\n{top_lines}"
    )

async def pending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    orders = ORDERS_DB.get("orders", {})
    pend = [(oid, o) for oid, o in orders.items() if o.get("status") == "pending"]
    if not pend:
        await update.message.reply_text("Нет ожидающих чеков.")
        return
    lines = ["🧾 Ожидают подтверждения:"]
    for oid, o in sorted(pend, key=lambda x: int(x[0])):
        lines.append(f"№{oid} | {o.get('user_label')} | {o.get('product_title')} | {format_money(o.get('price'))}")
    await update.message.reply_text("\n".join(lines))

async def confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not context.args:
        await update.message.reply_text("Использование: /confirm НОМЕР")
        return
    oid = context.args[0].strip()
    await _confirm_order(context, oid, notify_admin=True, admin_message=update.message)

async def reject(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not context.args:
        await update.message.reply_text("Использование: /reject НОМЕР")
        return
    oid = context.args[0].strip()
    await _reject_order(context, oid, notify_admin=True, admin_message=update.message)

# =====================================================
# Internal confirm/reject logic
# =====================================================
async def _confirm_order(context: ContextTypes.DEFAULT_TYPE, oid: str, notify_admin: bool = False, admin_message=None):
    order = ORDERS_DB.get("orders", {}).get(oid)
    if not order:
        if notify_admin and admin_message:
            await admin_message.reply_text(f"Заказ №{oid} не найден.")
        return

    if order.get("status") != "pending":
        if notify_admin and admin_message:
            await admin_message.reply_text(f"Заказ №{oid} не в статусе pending.")
        return

    # фиксируем промокод только после подтверждения оплаты
    promo_used = (order.get("promo") or "").strip()
    if promo_used:
        use_promo(promo_used)

    user_id = order.get("user_id")
    product_key = order.get("product")
    product = PRODUCTS.get(product_key) if product_key else None
    ptype = product.get("type") if product else None

    if not user_id:
        return

    if ptype == "ready":
        delivery_text = ""
        if product and product.get("delivery_doc"):
            delivery_text = get_doc_by_name(product["delivery_doc"])

        if not delivery_text:
            delivery_text = (
                "📦 Выдача:\n"
                "Ссылка/файл не задан.\n"
                "Добавь knowledge/delivery_gistology_ready.txt и обнови index_kb.py"
            )

        await context.bot.send_message(
            chat_id=user_id,
            text=(
                f"✅ Оплата подтверждена!\n\n"
                f"📚 Ваш заказ №{oid}:\n"
                f"{product.get('title') if product else ''}\n\n"
                f"{delivery_text}\n\n"
                "⭐️ После получения, пожалуйста, оставьте отзыв 🙏"
            ),
            reply_markup=review_keyboard(),
        )
        
        order["status"] = "delivered"
        order["updated_at"] = now_iso()
        save_orders(ORDERS_DB)
        return

    title = order.get("product_title") or (product.get("title") if product else "Индивидуальная работа")
    details = order.get("details") or {}
    deadline = details.get("deadline") if isinstance(details, dict) else None

    order["status"] = "inwork"
    order["updated_at"] = now_iso()
    save_orders(ORDERS_DB)

    deadline_line = f"\n⏰ Срок: {deadline}" if deadline else ""

    await context.bot.send_message(
        chat_id=user_id,
        text=(
            "✅ Оплата подтверждена!\n\n"
            f"📝 Заявка №{oid} принята в работу: {title}{deadline_line}\n"
            "📌 Если понадобится уточнение — мы напишем.\n"
            "⏳ Как будет готово — отправим сюда.\n\n"
            "⭐️ После получения сможете оставить отзыв 🙏"
        ),
        reply_markup=main_menu_keyboard(False),
    )

async def _reject_order(context: ContextTypes.DEFAULT_TYPE, oid: str, notify_admin: bool = False, admin_message=None):
    order = ORDERS_DB.get("orders", {}).get(oid)
    if not order:
        if notify_admin and admin_message:
            await admin_message.reply_text(f"Заказ №{oid} не найден.")
        return

    if order.get("status") != "pending":
        if notify_admin and admin_message:
            await admin_message.reply_text(f"Заказ №{oid} не в статусе pending.")
        return

    order["status"] = "rejected"
    order["updated_at"] = now_iso()
    save_orders(ORDERS_DB)

    user_id = order.get("user_id")
    if user_id:
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                f"⚠️ Не удалось подтвердить оплату по заказу №{oid}.\n\n"
                "Пожалуйста, отправьте *полный* чек (чтобы было видно сумму и дату/время) "
                "или нажмите «🆘 Поддержка»."
            ),
            reply_markup=main_menu_keyboard(False),
        )

    if notify_admin and admin_message:
        await admin_message.reply_text(f"❌ Заказ №{oid} отклонён. Клиенту отправлено сообщение.")

# =====================================================
# Main
# =====================================================
async def post_init(application: Application):
    """Запускает фоновую задачу после инициализации приложения"""
    asyncio.create_task(unpaid_reminder(application))

def main():
    logging.basicConfig(level=logging.INFO)
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()

    # user
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("myid", myid))
    
    # admin panel command
    app.add_handler(CommandHandler("admin", admin_panel))
    app.add_handler(CommandHandler("broadcast", broadcast))

    # admin commands
    app.add_handler(CommandHandler("msg", msg))
    app.add_handler(CommandHandler("reply", reply))
    app.add_handler(CommandHandler("setprice", setprice))
    app.add_handler(CommandHandler("pending", pending))
    app.add_handler(CommandHandler("confirm", confirm))
    app.add_handler(CommandHandler("reject", reject))
    app.add_handler(CommandHandler("inwork", inwork))
    app.add_handler(CommandHandler("ready", ready))
    app.add_handler(CommandHandler("delivered", delivered))
    app.add_handler(CommandHandler("stats", stats))

    # receipt photo
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))

    # text
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("✅ Бот с профессиональными функциями запущен")
    print("👨‍💼 Для админа: /admin")
    print("📢 Рассылка: /broadcast Текст")
    print("⭐ Отзывы автоматически приходят админу")
    print("⏰ Напоминания об оплате каждые 3 часа")
    print("🚫 Прогрессивный бан: 5 → 10 → 15 → 30 → 45 → 60 мин → PERM")
    print("👮‍♂️ Бан: кнопка в админке")
    print("♻ Разбан: кнопка в админке")
    print("🧹 Снять бан (спам): только для авто-спама")
    app.run_polling()

if __name__ == "__main__":
    main()
