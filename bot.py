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

TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
ADMIN_ID = (os.getenv("ADMIN_ID") or "").strip()
ADMIN_ID_INT = int(ADMIN_ID) if ADMIN_ID.isdigit() else None

if not TELEGRAM_BOT_TOKEN:
    print("❌ TELEGRAM_BOT_TOKEN не найден")
    raise SystemExit(1)

MBANK_REKV_FALLBACK = (os.getenv("MBANK_REKV_FALLBACK") or "").strip()

# =====================================================
# PERSISTENT STORAGE PATHS
# =====================================================
DATA_DIR = Path(".")
ORDERS_PATH = DATA_DIR / "orders.json"
BANS_PATH = DATA_DIR / "bans.json"
USERS_PATH = DATA_DIR / "users.json"
PROMO_PATH = DATA_DIR / "promo_codes.json"
PRODUCTS_PATH = DATA_DIR / "products.json"
KB_PATH = DATA_DIR / "kb_index.json"

# =====================================================
# BAN SYSTEM + ANTI-SPAM
# =====================================================
BAN_STEPS = [5, 10, 15, 30, 45, 60]  # minutes
SPAM_TRACKER = {}                   # user_id -> [datetime,...]
SPAM_LIMIT = 5
SPAM_SECONDS = 10

def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def save_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

BANS = load_json(BANS_PATH, {})            # { "user_id": {...} }
USERS_DB = load_json(USERS_PATH, [])       # [user_id, ...]

# =====================================================
# Knowledge Base (kb_index.json)
# =====================================================
KB_ITEMS = load_json(KB_PATH, {}).get("items", []) if KB_PATH.exists() else []

def get_doc_by_name(filename: str) -> str:
    parts = [it.get("text", "") for it in KB_ITEMS if it.get("source") == filename]
    return ("\n\n".join(parts)).strip()

# =====================================================
# Orders storage (orders.json)
# =====================================================
ORDERS_DB = load_json(ORDERS_PATH, {"last_id": 0, "orders": {}})

def save_orders():
    save_json(ORDERS_PATH, ORDERS_DB)

def new_order_id() -> int:
    ORDERS_DB["last_id"] = int(ORDERS_DB.get("last_id", 0)) + 1
    save_orders()
    return ORDERS_DB["last_id"]

def now_iso():
    return datetime.now().isoformat(timespec="seconds")

def parse_iso(dt_str: str):
    try:
        return datetime.fromisoformat(dt_str)
    except Exception:
        return None

# =====================================================
# Products / Catalog (persisted)
# =====================================================
PRODUCTS_DEFAULT = {
    "gistology_ready": {
        "title": "📚 СРС по гистологии (1–2 модуль) — комплект",
        "type": "ready",  # ready | individual
        "price": 499,
        "delivery_doc": "delivery_gistology_ready.txt",
    },
    "kahoot": {"title": "🧠 Kahoot (индивидуально)", "type": "individual"},
    "srs": {"title": "📚 СРС (самостоятельная работа)", "type": "individual"},
    "referat": {"title": "📄 Реферат", "type": "individual"},
    "doklad": {"title": "📘 Доклад", "type": "individual"},
    "presentation": {"title": "📊 Презентация (PowerPoint)", "type": "individual"},
}

PRODUCTS = load_json(PRODUCTS_PATH, None)
if not isinstance(PRODUCTS, dict) or not PRODUCTS:
    PRODUCTS = PRODUCTS_DEFAULT
    save_json(PRODUCTS_PATH, PRODUCTS)

# =====================================================
# Pricing templates (auto suggestion)
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
PROMO_CODES = load_json(PROMO_PATH, {})
# add built-in auto 5% code (unlimited)
PROMO_CODES.setdefault("AUTO5", {"discount": 5, "expires": None, "limit": 10**9, "used": 0})
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
    # allow "5" / "5%" as shorthand
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
    if disc <= 0:
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

def apply_promo(price: int, promo: str):
    disc, err = validate_promo(promo)
    if disc is None:
        return price, 0
    use_promo(promo)
    new_price = int(round(price * (100 - disc) / 100))
    return new_price, disc

# =====================================================
# Keyboards
# =====================================================
def main_menu_keyboard():
    rows = [
        [KeyboardButton("🛒 Покупка"), KeyboardButton("ℹ️ Инфо")],
        [KeyboardButton("🆘 Поддержка"), KeyboardButton("📌 Статус заказа")],
    ]
    if ADMIN_ID_INT:
        rows.append([KeyboardButton("🛠 Админ-панель")])
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
    # строим из PRODUCTS, чтобы новые товары автоматически появлялись
    buttons = []
    # Сначала готовый товар, потом остальное
    ready = [p for p in PRODUCTS.values() if p.get("type") == "ready"]
    individual = [p for p in PRODUCTS.values() if p.get("type") != "ready"]
    for p in ready + individual:
        buttons.append([KeyboardButton(p.get("title", "Товар"))])
    buttons.append([KeyboardButton("⬅️ Назад")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def info_menu_keyboard():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("💳 Оплата"), KeyboardButton("⭐️ Оставить отзыв")],
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

def admin_panel_keyboard():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("🧾 Чеки (pending)")],
            [KeyboardButton("✅ Подтвердить"), KeyboardButton("❌ Отклонить")],
            [KeyboardButton("🟡 В работу"), KeyboardButton("🟢 Готово")],
            [KeyboardButton("📩 Выдать (отправить файл/ссылку)")],
            [KeyboardButton("💰 Выставить цену")],
            [KeyboardButton("💬 Ответ клиенту")],
            [KeyboardButton("➕ Добавить товар"), KeyboardButton("➖ Удалить товар")],
            [KeyboardButton("🎟➕ Добавить промокод"), KeyboardButton("🎟➖ Удалить промокод")],
            [KeyboardButton("📢 Рассылка")],
            [KeyboardButton("🚫 Забанить"), KeyboardButton("♻ Разбанить"), KeyboardButton("🧹 Снять бан (спам)")],
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
    m = re.search(r"(\d{1,9})", (text or "").replace(" ", ""))
    return int(m.group(1)) if m else None

def product_key_from_title(title: str):
    for k, v in PRODUCTS.items():
        if v.get("title") == title:
            return k
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

def order_status_human(status: str):
    mapping = {
        "needs_pricing": "⏳ Ожидает расчёта стоимости",
        "priced": "💳 Ожидает оплату",
        "reminded": "⏰ Напомнили об оплате",
        "pending": "🧾 Чек на проверке",
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
# Background: reminder about unpaid orders
# =====================================================
async def unpaid_reminder_job(context: ContextTypes.DEFAULT_TYPE):
    """JobQueue: remind after 3h if still priced."""
    now = datetime.now()
    updated = False
    for oid, order in ORDERS_DB.get("orders", {}).items():
        if order.get("status") != "priced":
            continue
        created = parse_iso(order.get("created_at") or order.get("updated_at") or "")
        if not created:
            continue
        if (now - created).total_seconds() < 10800:  # 3h
            continue
        user_id = order.get("user_id")
        if not user_id:
            continue
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    f"⏰ Напоминание!\n\n"
                    f"Заказ №{oid} ещё не оплачен.\n"
                    f"Если нужна помощь — нажмите «🆘 Поддержка».\n\n"
                    f"🎁 Бонус: можете применить промо «5%» (скидка 5%)."
                ),
                reply_markup=main_menu_keyboard(),
            )
            order["status"] = "reminded"
            order["updated_at"] = now_iso()
            updated = True
        except Exception as e:
            logging.error(f"Reminder error for {user_id}: {e}")
    if updated:
        save_orders()

# =====================================================
# Start
# =====================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # save user
    uid = update.effective_user.id
    if uid not in USERS_DB:
        USERS_DB.append(uid)
        save_json(USERS_PATH, USERS_DB)

    # keep promo between restarts of /start
    promo = context.user_data.get("promo_default")
    context.user_data.clear()
    if promo:
        context.user_data["promo_default"] = promo

    await update.message.reply_text(
        "👋 Добро пожаловать в StubHub!\n\n"
        "📚 Здесь можно заказать работу или купить готовый комплект.\n\n"
        "Выберите действие 👇",
        reply_markup=main_menu_keyboard(),
    )

# =====================================================
# Broadcast
# =====================================================
async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    text = " ".join(context.args).strip()
    if not text:
        await update.message.reply_text("Использование: /broadcast Текст сообщения")
        return

    users = USERS_DB[:]
    ok = 0
    bad = 0
    await update.message.reply_text(f"📢 Рассылка: {len(users)} пользователей...")
    for uid in users:
        try:
            await context.bot.send_message(uid, f"📢 StubHub:\n\n{text}", reply_markup=main_menu_keyboard())
            ok += 1
            await asyncio.sleep(0.05)
        except Exception:
            bad += 1
    await update.message.reply_text(f"✅ Готово. Успешно: {ok}, ошибок: {bad}")

# =====================================================
# FORM for individual services
# =====================================================
FORM_QUESTIONS = [
    ("topic", "📝 Напишите тему."),
    ("volume", "📏 Укажите объём (страницы / слайды / количество вопросов)."),
    ("reqs", "📌 Требования (оформление/методичка/стиль). Если нет — напишите «нет»."),
    ("deadline", "⏰ Срок сдачи (дата/когда нужно)."),
    ("promo", "🎟 Если есть промокод — отправьте его. Если нет — напишите «нет»."),
]

def form_reset(context: ContextTypes.DEFAULT_TYPE):
    for k in ("form_step", "form_data", "selected_product"):
        context.user_data.pop(k, None)

async def form_start(update: Update, context: ContextTypes.DEFAULT_TYPE, product_key: str):
    context.user_data["selected_product"] = product_key
    context.user_data["form_step"] = 0
    context.user_data["form_data"] = {}
    await update.message.reply_text(
        f"✅ Вы выбрали: {PRODUCTS[product_key]['title']}\n\n"
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

    key, _ = FORM_QUESTIONS[step]
    form_data[key] = user_text.strip()
    context.user_data["form_data"] = form_data

    step += 1
    if step < len(FORM_QUESTIONS):
        context.user_data["form_step"] = step
        await update.message.reply_text(FORM_QUESTIONS[step][1])
        return True

    # finalize
    context.user_data["form_step"] = None

    topic = form_data.get("topic", "")
    volume = form_data.get("volume", "")
    reqs = form_data.get("reqs", "")
    deadline = form_data.get("deadline", "")
    promo = (form_data.get("promo", "") or "").strip()
    promo = "" if promo.lower() in ("нет", "no", "-") else promo.upper()

    suggested_price, breakdown = calc_suggested_price(product_key, volume)
    sp2, pct = (suggested_price, 0)
    if isinstance(suggested_price, int) and promo:
        sp2, pct = apply_promo(suggested_price, promo)

    oid = new_order_id()
    u = update.effective_user
    product_title = PRODUCTS.get(product_key, {}).get("title", product_key)

    ORDERS_DB["orders"][str(oid)] = {
        "status": "needs_pricing",
        "user_id": u.id if u else None,
        "user_label": user_label(update),
        "product": product_key,
        "product_title": product_title,
        "details": {"topic": topic, "volume": volume, "reqs": reqs, "deadline": deadline},
        "promo": promo if promo else None,
        "promo_pct": pct,
        "suggested_price": sp2,
        "suggested_breakdown": breakdown,
        "created_at": now_iso(),
        "updated_at": now_iso(),
    }
    save_orders()

    await update.message.reply_text(
        f"✅ Заявка принята! Номер: №{oid}\n\n"
        "Сейчас рассчитаю стоимость и напишу вам.\n"
        "Если нужно срочно — нажмите «🆘 Поддержка».",
        reply_markup=buy_menu_keyboard(),
    )

    # notify admin
    if ADMIN_ID_INT is not None:
        sug_line = f"\n💡 Автоцена: {sp2} сом ({breakdown})" if isinstance(sp2, int) else ""
        promo_line = f"\n🎟 Промокод: {promo} (-{pct}%)" if promo and pct else (f"\n🎟 Промокод: {promo} (не найден)" if promo else "")
        await context.bot.send_message(
            chat_id=ADMIN_ID_INT,
            text=(
                f"🆕 Новая заявка №{oid}\n"
                f"Клиент: {user_label(update)}\n"
                f"User ID: {u.id if u else 'unknown'}\n"
                f"Услуга: {product_title}"
                f"{sug_line}{promo_line}\n\n"
                f"• Тема: {topic}\n"
                f"• Объём: {volume}\n"
                f"• Требования: {reqs}\n"
                f"• Срок: {deadline}\n\n"
                f"Выставить цену: /setprice {oid} 700"
            ),
        )

    form_reset(context)
    return True

# =====================================================
# Receipt photo handler (customer) OR delivery photo/doc (admin)
# =====================================================
async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # if admin is sending material to customer
    if is_admin(update) and context.user_data.get("send_file_order"):
        oid = context.user_data.get("send_file_order")
        await _admin_send_material(update, context, oid, kind="photo")
        return

    # customer receipt
    oid = context.user_data.get("awaiting_receipt_order_id")
    if not oid:
        await update.message.reply_text(
            "📷 Фото получено.\nЕсли это чек — нажмите кнопку «💳 Я оплатил(а) №...».",
            reply_markup=main_menu_keyboard(),
        )
        return

    context.user_data["awaiting_receipt_order_id"] = None
    order = ORDERS_DB.get("orders", {}).get(str(oid))
    if not order:
        await update.message.reply_text("❌ Заказ не найден.", reply_markup=main_menu_keyboard())
        return

    if order.get("user_id") != update.effective_user.id:
        await update.message.reply_text("❌ Это не ваш заказ.", reply_markup=main_menu_keyboard())
        return

    if order.get("status") != "priced":
        await update.message.reply_text(
            f"По заказу №{oid} сейчас статус: {order_status_human(order.get('status'))}.",
            reply_markup=main_menu_keyboard(),
        )
        return

    order["status"] = "pending"
    order["updated_at"] = now_iso()
    save_orders()

    await update.message.reply_text("✅ Чек получен и отправлен на проверку.", reply_markup=main_menu_keyboard())

    if ADMIN_ID_INT is None:
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

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_admin(update) and context.user_data.get("send_file_order"):
        oid = context.user_data.get("send_file_order")
        await _admin_send_material(update, context, oid, kind="document")
        return
    # if customer sends receipt as document, treat as receipt too
    oid = context.user_data.get("awaiting_receipt_order_id")
    if oid:
        # forward document to admin
        order = ORDERS_DB.get("orders", {}).get(str(oid))
        if order and order.get("user_id") == update.effective_user.id and order.get("status") == "priced":
            order["status"] = "pending"
            order["updated_at"] = now_iso()
            save_orders()
            await update.message.reply_text("✅ Чек получен и отправлен на проверку.", reply_markup=main_menu_keyboard())
            if ADMIN_ID_INT is not None:
                try:
                    await update.message.forward(chat_id=ADMIN_ID_INT)
                except Exception:
                    pass
                await context.bot.send_message(
                    chat_id=ADMIN_ID_INT,
                    text=(
                        f"🧾 Новый чек (документ)\n"
                        f"№{oid}\n"
                        f"Клиент: {order.get('user_label')}\n"
                        f"User ID: {order.get('user_id')}\n"
                        f"Товар/услуга: {order.get('product_title')}\n"
                        f"Сумма: {format_money(order.get('price'))}"
                    ),
                )
            context.user_data["awaiting_receipt_order_id"] = None
            return
    await update.message.reply_text("📎 Документ получен.", reply_markup=main_menu_keyboard())

async def _admin_send_material(update: Update, context: ContextTypes.DEFAULT_TYPE, oid: str, kind: str):
    order = ORDERS_DB.get("orders", {}).get(str(oid))
    if not order:
        await update.message.reply_text("❌ Заказ не найден.", reply_markup=admin_panel_keyboard())
        context.user_data.pop("send_file_order", None)
        return

    user_id = order.get("user_id")
    if not user_id:
        await update.message.reply_text("❌ У заказа нет user_id.", reply_markup=admin_panel_keyboard())
        context.user_data.pop("send_file_order", None)
        return

    # send to user
    if kind == "document" and update.message.document:
        await context.bot.send_document(
            chat_id=user_id,
            document=update.message.document.file_id,
            caption=f"📩 Ваш заказ №{oid} готов!\n\n⭐️ Будем рады отзыву 🙏",
            reply_markup=review_keyboard(),
        )
    elif kind == "photo" and update.message.photo:
        await context.bot.send_photo(
            chat_id=user_id,
            photo=update.message.photo[-1].file_id,
            caption=f"📩 Ваш заказ №{oid} готов!\n\n⭐️ Будем рады отзыву 🙏",
            reply_markup=review_keyboard(),
        )
    else:
        # fallback: text link
        text = (update.message.text or "").strip()
        if not text:
            await update.message.reply_text("Отправьте файл/фото или текст-ссылку.", reply_markup=admin_panel_keyboard())
            return
        await context.bot.send_message(
            chat_id=user_id,
            text=f"📩 Ваш заказ №{oid} готов:\n\n{text}\n\n⭐️ Будем рады отзыву 🙏",
            reply_markup=review_keyboard(),
        )

    order["status"] = "delivered"
    order["updated_at"] = now_iso()
    save_orders()

    await update.message.reply_text("✅ Отправлено клиенту.", reply_markup=admin_panel_keyboard())
    context.user_data.pop("send_file_order", None)

# =====================================================
# Admin command handlers
# =====================================================
async def pending_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    pend = [(oid, o) for oid, o in ORDERS_DB.get("orders", {}).items() if o.get("status") == "pending"]
    if not pend:
        await update.message.reply_text("Нет ожидающих чеков.", reply_markup=admin_panel_keyboard())
        return
    lines = ["🧾 Ожидают подтверждения:"]
    for oid, o in sorted(pend, key=lambda x: int(x[0])):
        lines.append(f"№{oid} | {o.get('user_label')} | {o.get('product_title')} | {format_money(o.get('price'))}")
    await update.message.reply_text("\n".join(lines), reply_markup=admin_panel_keyboard())

async def reply_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    await context.bot.send_message(chat_id=user_id, text=f"👨‍💼 Менеджер:\n{text}", reply_markup=main_menu_keyboard())
    await update.message.reply_text("✅ Отправлено.")

async def setprice_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not context.args:
        await update.message.reply_text("Использование: /setprice НОМЕР СУММА")
        return
    oid = context.args[0].strip()
    raw = " ".join(context.args[1:])
    price = extract_first_int(raw)
    if price is None:
        await update.message.reply_text("Сумма должна содержать число (например 700).")
        return
    order = ORDERS_DB.get("orders", {}).get(oid)
    if not order:
        await update.message.reply_text(f"Заказ №{oid} не найден.")
        return
    if order.get("status") not in ("needs_pricing", "priced", "reminded"):
        await update.message.reply_text(f"Заказ №{oid} уже: {order_status_human(order.get('status'))}.")
        return
    promo = (order.get("promo") or "").strip()
    price2, pct = apply_promo(int(price), promo) if promo else (int(price), 0)
    order["price"] = int(price2)
    order["promo_pct"] = pct
    order["status"] = "priced"
    order["updated_at"] = now_iso()
    save_orders()

    pay = get_doc_by_name("payment.txt") or MBANK_REKV_FALLBACK or "💳 Оплата через MBank: 📌 +996999888332 Имронбек С."
    promo_line = f"\n🎟 Промокод: {promo} (-{pct}%)" if promo and pct else ""
    await context.bot.send_message(
        chat_id=order["user_id"],
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

async def _confirm_order(context: ContextTypes.DEFAULT_TYPE, oid: str):
    order = ORDERS_DB.get("orders", {}).get(str(oid))
    if not order:
        return False, "Заказ не найден."
    if order.get("status") != "pending":
        return False, f"Статус сейчас: {order_status_human(order.get('status'))}."

    user_id = order.get("user_id")
    product_key = order.get("product")
    product = PRODUCTS.get(product_key) if product_key else None
    ptype = product.get("type") if product else None

    if not user_id:
        return False, "У заказа нет user_id."

    if ptype == "ready":
        delivery_text = ""
        if product and product.get("delivery_doc"):
            delivery_text = get_doc_by_name(product["delivery_doc"])
        if not delivery_text:
            delivery_text = "📦 Выдача не настроена (нет delivery_doc в knowledge)."

        await context.bot.send_message(
            chat_id=user_id,
            text=(
                f"✅ Оплата подтверждена!\n\n"
                f"📚 Ваш заказ №{oid}:\n{product.get('title')}\n\n"
                f"{delivery_text}\n\n"
                "⭐️ После получения, пожалуйста, оставьте отзыв 🙏"
            ),
            reply_markup=review_keyboard(),
        )
        order["status"] = "delivered"
        order["updated_at"] = now_iso()
        save_orders()
        return True, "Готовый товар выдан."

    # individual -> in work
    order["status"] = "inwork"
    order["updated_at"] = now_iso()
    save_orders()
    title = order.get("product_title") or "Индивидуальная работа"
    details = order.get("details") or {}
    deadline = details.get("deadline") if isinstance(details, dict) else None
    deadline_line = f"\n⏰ Срок: {deadline}" if deadline else ""
    await context.bot.send_message(
        chat_id=user_id,
        text=(
            "✅ Оплата подтверждена!\n\n"
            f"📝 Заявка №{oid} принята в работу: {title}{deadline_line}\n"
            "⏳ Как будет готово — отправим сюда.\n\n"
            "⭐️ После получения сможете оставить отзыв 🙏"
        ),
        reply_markup=main_menu_keyboard(),
    )
    return True, "Переведено в работу."

async def _reject_order(context: ContextTypes.DEFAULT_TYPE, oid: str):
    order = ORDERS_DB.get("orders", {}).get(str(oid))
    if not order:
        return False, "Заказ не найден."
    if order.get("status") != "pending":
        return False, f"Статус сейчас: {order_status_human(order.get('status'))}."
    order["status"] = "rejected"
    order["updated_at"] = now_iso()
    save_orders()
    uid = order.get("user_id")
    if uid:
        await context.bot.send_message(
            chat_id=uid,
            text=(
                f"⚠️ Не удалось подтвердить оплату по заказу №{oid}.\n\n"
                "Пожалуйста, отправьте полный чек или нажмите «🆘 Поддержка»."
            ),
            reply_markup=main_menu_keyboard(),
        )
    return True, "Отклонено."

async def confirm_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not context.args:
        await update.message.reply_text("Использование: /confirm НОМЕР")
        return
    ok, msg = await _confirm_order(context, context.args[0].strip())
    await update.message.reply_text(("✅ " if ok else "❌ ") + msg)

async def reject_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not context.args:
        await update.message.reply_text("Использование: /reject НОМЕР")
        return
    ok, msg = await _reject_order(context, context.args[0].strip())
    await update.message.reply_text(("✅ " if ok else "❌ ") + msg)

async def inwork_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    oid = (context.args[0] if context.args else "").strip()
    if not oid:
        await update.message.reply_text("Использование: /inwork НОМЕР")
        return
    order = ORDERS_DB.get("orders", {}).get(oid)
    if not order:
        await update.message.reply_text("Не найден.")
        return
    order["status"] = "inwork"
    order["updated_at"] = now_iso()
    save_orders()
    uid = order.get("user_id")
    if uid:
        await context.bot.send_message(uid, f"🟡 Заказ №{oid} взят в работу.", reply_markup=main_menu_keyboard())
    await update.message.reply_text("✅ Статус: в работе.")

async def ready_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    oid = (context.args[0] if context.args else "").strip()
    if not oid:
        await update.message.reply_text("Использование: /ready НОМЕР")
        return
    order = ORDERS_DB.get("orders", {}).get(oid)
    if not order:
        await update.message.reply_text("Не найден.")
        return
    order["status"] = "ready"
    order["updated_at"] = now_iso()
    save_orders()
    uid = order.get("user_id")
    if uid:
        await context.bot.send_message(uid, f"🟢 Заказ №{oid} готов! Скоро отправим.", reply_markup=main_menu_keyboard())
    await update.message.reply_text("✅ Статус: готово.")

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    period = (context.args[0].lower() if context.args else "day")
    now = datetime.now()
    if period == "week":
        start = now - timedelta(days=7); label = "за 7 дней"
    elif period == "month":
        start = now - timedelta(days=30); label = "за 30 дней"
    else:
        start = now - timedelta(days=1); label = "за 24 часа"

    orders = ORDERS_DB.get("orders", {})
    count = paid = paid_sum = 0
    for _oid, o in orders.items():
        dt = parse_iso(o.get("created_at", ""))
        if not dt or dt < start:
            continue
        count += 1
        if o.get("status") in ("inwork", "ready", "delivered"):
            paid += 1
            if isinstance(o.get("price"), int):
                paid_sum += o["price"]
    avg = int(round(paid_sum / paid)) if paid else 0
    await update.message.reply_text(
        f"📊 Статистика {label}\n\n"
        f"Заявок: {count}\n"
        f"Оплачено (подтв.): {paid}\n"
        f"Сумма оплат: {paid_sum} сом\n"
        f"Средний чек: {avg} сом",
        reply_markup=admin_panel_keyboard(),
    )

# =====================================================
# Admin panel (keyboard-only) logic
# =====================================================
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    # clear any pending admin flows
    context.user_data.pop("admin_action", None)
    context.user_data.pop("send_file_order", None)
    await update.message.reply_text("🛠 Админ-панель:", reply_markup=admin_panel_keyboard())

# =====================================================
# Main message handler
# =====================================================
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_text = (update.message.text or "").strip()
    if not user_text:
        return
    lower_text = user_text.lower()
    uid = update.effective_user.id
    now = datetime.now()

    # ---------------- BAN CHECK ----------------
    ban = BANS.get(str(uid))
    if ban:
        if ban.get("type") == "perm":
            reason = ban.get("reason", "без причины")
            await update.message.reply_text(
                f"🚫 Вы навсегда заблокированы.\nПричина: {reason}\n\nЕсли ошибка — @slt_nv"
            )
            return
        if ban.get("type") == "temp":
            until = parse_iso(ban.get("until") or "")
            if until and now < until:
                remaining = max(1, int((until - now).total_seconds() / 60))
                await update.message.reply_text(
                    f"🚫 Вы заблокированы за спам.\nПопробуйте через {remaining} минут.\n\nЕсли ошибка — @slt_nv"
                )
                return
            # expire
            BANS.pop(str(uid), None)
            save_json(BANS_PATH, BANS)

    # ---------------- ANTI-SPAM ----------------
    history = SPAM_TRACKER.get(uid, [])
    history = [t for t in history if (now - t).total_seconds() < SPAM_SECONDS]
    history.append(now)
    SPAM_TRACKER[uid] = history
    if len(history) > SPAM_LIMIT:
        strikes = int(BANS.get(str(uid), {}).get("strikes", 0)) + 1
        if strikes > len(BAN_STEPS):
            BANS[str(uid)] = {"type": "perm", "reason": "спам", "strikes": strikes}
            save_json(BANS_PATH, BANS)
            await update.message.reply_text(
                "🚫 Вы навсегда заблокированы.\nПричина: спам\n\nЕсли ошибка — @slt_nv"
            )
            if ADMIN_ID_INT:
                await context.bot.send_message(ADMIN_ID_INT, f"🚫 PERM BAN\nUser ID: {uid}\nПричина: спам")
            return
        minutes = BAN_STEPS[strikes - 1]
        BANS[str(uid)] = {
            "type": "temp",
            "until": (now + timedelta(minutes=minutes)).isoformat(),
            "reason": "спам",
            "strikes": strikes,
        }
        save_json(BANS_PATH, BANS)
        await update.message.reply_text(
            f"🚫 Вы заблокированы за спам.\nСрок: {minutes} минут.\n\nЕсли ошибка — @slt_nv"
        )
        return

    # ---------------- ADMIN PANEL BUTTON FLOW ----------------
    if is_admin(update):
        # open panel
        if user_text == "🛠 Админ-панель":
            await admin_panel(update, context)
            return

        # handle "send material" text link after clicking "Выдать..."
        if context.user_data.get("send_file_order") and user_text:
            oid = context.user_data.get("send_file_order")
            await _admin_send_material(update, context, oid, kind="text")
            return

        if user_text == "🧾 Чеки (pending)":
            await pending_cmd(update, context); return
        if user_text == "📊 Статистика":
            await stats_cmd(update, context); return
        if user_text == "⬅️ В меню":
            await update.message.reply_text("🏠 Главное меню:", reply_markup=main_menu_keyboard()); return

        # set action prompts (IMPORTANT: reset previous order, so no random ids!)
        if user_text in ("✅ Подтвердить", "❌ Отклонить", "🟡 В работу", "🟢 Готово"):
            mapping = {
                "✅ Подтвердить": "confirm",
                "❌ Отклонить": "reject",
                "🟡 В работу": "inwork",
                "🟢 Готово": "ready",
            }
            context.user_data["admin_action"] = mapping[user_text]
            context.user_data.pop("send_file_order", None)
            await update.message.reply_text("Введите номер заказа:", reply_markup=ReplyKeyboardRemove())
            return

        if user_text == "📩 Выдать (отправить файл/ссылку)":
            context.user_data["admin_action"] = "deliver"
            context.user_data.pop("send_file_order", None)
            await update.message.reply_text("Введите номер заказа:", reply_markup=ReplyKeyboardRemove())
            return

        if user_text == "💰 Выставить цену":
            context.user_data["admin_action"] = "setprice"
            await update.message.reply_text("Введите: НОМЕР СУММА  (пример: 25 700)", reply_markup=ReplyKeyboardRemove())
            return

        if user_text == "💬 Ответ клиенту":
            context.user_data["admin_action"] = "reply"
            await update.message.reply_text("Введите: НОМЕР Текст (пример: 25 Готово)", reply_markup=ReplyKeyboardRemove())
            return

        if user_text == "📢 Рассылка":
            context.user_data["admin_action"] = "broadcast"
            await update.message.reply_text("Введите текст для рассылки:", reply_markup=ReplyKeyboardRemove())
            return

        if user_text == "🚫 Забанить":
            context.user_data["admin_action"] = "ban"
            await update.message.reply_text("Введите: USER_ID причина (пример: 123456 грубость)", reply_markup=ReplyKeyboardRemove())
            return

        if user_text == "♻ Разбанить":
            context.user_data["admin_action"] = "unban"
            await update.message.reply_text("Введите USER_ID:", reply_markup=ReplyKeyboardRemove())
            return

        if user_text == "🧹 Снять бан (спам)":
            context.user_data["admin_action"] = "unban_spam"
            await update.message.reply_text("Введите USER_ID:", reply_markup=ReplyKeyboardRemove())
            return

        if user_text == "➕ Добавить товар":
            context.user_data["admin_action"] = "add_product"
            await update.message.reply_text(
                "Формат:\nkey|Название|type(ready/individual)|price(если ready)|delivery_doc(если ready)\n"
                "Пример:\nnewpack|📦 Новый комплект|ready|399|delivery_newpack.txt",
                reply_markup=ReplyKeyboardRemove(),
            )
            return

        if user_text == "➖ Удалить товар":
            context.user_data["admin_action"] = "del_product"
            await update.message.reply_text("Введите key товара (например: kahoot):", reply_markup=ReplyKeyboardRemove())
            return

        if user_text == "🎟➕ Добавить промокод":
            context.user_data["admin_action"] = "add_promo"
            await update.message.reply_text(
                "Формат:\nCODE|DISCOUNT|YYYY-MM-DD(или пусто)|LIMIT\n"
                "Пример:\nSPRING10|10|2026-04-01|100",
                reply_markup=ReplyKeyboardRemove(),
            )
            return

        if user_text == "🎟➖ Удалить промокод":
            context.user_data["admin_action"] = "del_promo"
            await update.message.reply_text("Введите CODE промокода:", reply_markup=ReplyKeyboardRemove())
            return

        # process admin_action input
        action = context.user_data.get("admin_action")
        if action:
            # IMPORTANT: never reuse old order ids; always parse from current message
            if action in ("confirm", "reject", "inwork", "ready", "deliver"):
                oid = extract_first_int(user_text)
                if oid is None:
                    await update.message.reply_text("Введите корректный номер заказа.")
                    return
                oid = str(oid)

                if action == "confirm":
                    ok, msg = await _confirm_order(context, oid)
                    await update.message.reply_text(("✅ " if ok else "❌ ") + msg, reply_markup=admin_panel_keyboard())
                elif action == "reject":
                    ok, msg = await _reject_order(context, oid)
                    await update.message.reply_text(("✅ " if ok else "❌ ") + msg, reply_markup=admin_panel_keyboard())
                elif action == "inwork":
                    context.args = [oid]
                    await inwork_cmd(update, context)
                    await update.message.reply_text("✅ Готово.", reply_markup=admin_panel_keyboard())
                elif action == "ready":
                    context.args = [oid]
                    await ready_cmd(update, context)
                    await update.message.reply_text("✅ Готово.", reply_markup=admin_panel_keyboard())
                else:  # deliver
                    if oid not in ORDERS_DB.get("orders", {}):
                        await update.message.reply_text("❌ Заказ не найден.", reply_markup=admin_panel_keyboard())
                    else:
                        context.user_data["send_file_order"] = oid
                        await update.message.reply_text(
                            f"📎 Теперь отправьте файл/фото или текст-ссылку для заказа №{oid}.",
                            reply_markup=ReplyKeyboardRemove(),
                        )
                context.user_data.pop("admin_action", None)
                return

            if action == "setprice":
                parts = user_text.split()
                if len(parts) < 2:
                    await update.message.reply_text("Формат: НОМЕР СУММА (пример: 25 700)")
                    return
                context.args = [parts[0], " ".join(parts[1:])]
                await setprice_cmd(update, context)
                context.user_data.pop("admin_action", None)
                await update.message.reply_text("✅ Готово.", reply_markup=admin_panel_keyboard())
                return

            if action == "reply":
                parts = user_text.split(maxsplit=1)
                if len(parts) < 2:
                    await update.message.reply_text("Формат: НОМЕР Текст")
                    return
                context.args = parts
                await reply_cmd(update, context)
                context.user_data.pop("admin_action", None)
                await update.message.reply_text("✅ Готово.", reply_markup=admin_panel_keyboard())
                return

            if action == "broadcast":
                # send as command-like
                context.args = user_text.split()
                await broadcast_cmd(update, context)
                context.user_data.pop("admin_action", None)
                await update.message.reply_text("✅ Готово.", reply_markup=admin_panel_keyboard())
                return

            if action == "ban":
                parts = user_text.split(maxsplit=1)
                if len(parts) < 2:
                    await update.message.reply_text("Формат: USER_ID причина")
                    return
                ban_uid = parts[0].strip()
                reason = parts[1].strip()
                BANS[str(ban_uid)] = {"type": "perm", "reason": reason, "strikes": 999}
                save_json(BANS_PATH, BANS)
                try:
                    await context.bot.send_message(
                        chat_id=int(ban_uid),
                        text=(f"🚫 Вы навсегда заблокированы администратором.\n"
                              f"Причина: {reason}\n\nЕсли ошибка — @slt_nv")
                    )
                except Exception:
                    pass
                await update.message.reply_text("✅ Пользователь заблокирован.", reply_markup=admin_panel_keyboard())
                context.user_data.pop("admin_action", None)
                return

            if action == "unban":
                ban_uid = str(extract_first_int(user_text) or "").strip()
                if ban_uid and ban_uid in BANS:
                    BANS.pop(ban_uid, None)
                    save_json(BANS_PATH, BANS)
                await update.message.reply_text("✅ Пользователь разблокирован.", reply_markup=admin_panel_keyboard())
                context.user_data.pop("admin_action", None)
                return

            if action == "unban_spam":
                ban_uid = str(extract_first_int(user_text) or "").strip()
                if ban_uid in BANS and (BANS[ban_uid].get("reason") == "спам"):
                    BANS.pop(ban_uid, None)
                    save_json(BANS_PATH, BANS)
                    await update.message.reply_text("✅ Спам-бан снят.", reply_markup=admin_panel_keyboard())
                else:
                    await update.message.reply_text("⚠️ Нет активного спам-бана для этого ID.", reply_markup=admin_panel_keyboard())
                context.user_data.pop("admin_action", None)
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
                context.user_data.pop("admin_action", None)
                return

            if action == "del_product":
                key = user_text.strip()
                if key in PRODUCTS:
                    PRODUCTS.pop(key, None)
                    save_json(PRODUCTS_PATH, PRODUCTS)
                    await update.message.reply_text("✅ Товар удалён.", reply_markup=admin_panel_keyboard())
                else:
                    await update.message.reply_text("❌ Такой key не найден.", reply_markup=admin_panel_keyboard())
                context.user_data.pop("admin_action", None)
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
                context.user_data.pop("admin_action", None)
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
                context.user_data.pop("admin_action", None)
                return

    # ---------------- FORM IN PROGRESS ----------------
    if context.user_data.get("form_step") is not None:
        handled = await form_continue(update, context, user_text)
        if handled:
            return

    # ---------------- REVIEW ----------------
    if context.user_data.get("waiting_review"):
        context.user_data["waiting_review"] = False
        await update.message.reply_text("🙏 Спасибо за отзыв!", reply_markup=main_menu_keyboard())
        if ADMIN_ID_INT:
            u = update.effective_user
            await context.bot.send_message(
                ADMIN_ID_INT,
                f"⭐ Новый отзыв\n\nОт: {u.full_name}\n"
                f"Username: @{u.username if u.username else 'нет'}\nID: {u.id}\n\n{user_text}",
            )
        return

    # ---------------- NAVIGATION ----------------
    if user_text in ("🏠 В меню", "⬅️ Назад"):
        await update.message.reply_text("🏠 Главное меню:", reply_markup=main_menu_keyboard())
        return

    if user_text == "⬅️ В меню":
        await update.message.reply_text("🛒 Раздел покупок:", reply_markup=buy_menu_keyboard())
        return

    if user_text == "🛒 Покупка":
        context.user_data["support_mode"] = False
        context.user_data["support_order_id"] = None
        await update.message.reply_text("🛒 Раздел покупок:", reply_markup=buy_menu_keyboard())
        return

    if user_text == "📂 Каталог":
        context.user_data["support_mode"] = False
        context.user_data["support_order_id"] = None
        await update.message.reply_text("📦 Выберите товар/услугу:", reply_markup=catalog_keyboard())
        return

    if user_text == "ℹ️ Инфо":
        await update.message.reply_text("ℹ️ Информация:", reply_markup=info_menu_keyboard())
        return

    if user_text == "💳 Оплата":
        txt = get_doc_by_name("payment.txt") or MBANK_REKV_FALLBACK
        await update.message.reply_text(txt or "Добавь knowledge/payment.txt → python index_kb.py", reply_markup=info_menu_keyboard())
        return

    # promo
    if user_text == "🎟 Промокод":
        await update.message.reply_text(
            "Отправьте промокод одним сообщением.\n"
            "Можно просто написать «5%» для скидки 5%.\n\n"
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
            disc, err = validate_promo(promo_u)
            if disc:
                context.user_data["promo_default"] = promo_u
                await update.message.reply_text(f"✅ Промокод активирован (-{disc}%).", reply_markup=buy_menu_keyboard())
            else:
                context.user_data["promo_default"] = promo_u
                await update.message.reply_text(f"⚠️ {err}\nЯ сохраню — менеджер проверит.", reply_markup=buy_menu_keyboard())
        return

    # review start
    if user_text == "⭐️ Оставить отзыв":
        context.user_data["waiting_review"] = True
        await update.message.reply_text("✍️ Напишите ваш отзыв одним сообщением:", reply_markup=ReplyKeyboardRemove())
        return

    # order status
    if user_text == "📌 Статус заказа":
        oid, order = last_order_for_user(uid)
        if not order:
            await update.message.reply_text("Пока нет заказов. Откройте «📂 Каталог».", reply_markup=buy_menu_keyboard())
            return
        await update.message.reply_text(
            f"📌 Ваш последний заказ:\n"
            f"№{oid}\n"
            f"Услуга: {order.get('product_title')}\n"
            f"Статус: {order_status_human(order.get('status'))}\n"
            f"Сумма: {format_money(order.get('price'))}",
            reply_markup=main_menu_keyboard(),
        )
        return

    # support
    if user_text == "🆘 Поддержка" or "менеджер" in lower_text:
        oid = new_order_id()
        ORDERS_DB["orders"][str(oid)] = {
            "status": "support",
            "user_id": uid,
            "user_label": user_label(update),
            "product": context.user_data.get("selected_product"),
            "created_at": now_iso(),
            "updated_at": now_iso(),
        }
        save_orders()
        context.user_data["support_mode"] = True
        context.user_data["support_order_id"] = str(oid)
        await update.message.reply_text(
            f"Опишите проблему одним сообщением — я передам менеджеру.\n\n"
            f"📌 Номер обращения: №{oid}\n\n"
            f"Чтобы закрыть поддержку нажмите: «❌ Закрыть поддержку»",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton("❌ Закрыть поддержку")]], resize_keyboard=True),
        )
        if ADMIN_ID_INT is not None:
            await context.bot.send_message(
                chat_id=ADMIN_ID_INT,
                text=f"🆘 Новое обращение №{oid}\nКлиент: {user_label(update)}\nUser ID: {uid}\n\nОтветить: /reply {oid} текст",
            )
        return

    if user_text == "❌ Закрыть поддержку":
        context.user_data["support_mode"] = False
        context.user_data["support_order_id"] = None
        await update.message.reply_text("✅ Поддержка закрыта.", reply_markup=main_menu_keyboard())
        return

    if context.user_data.get("support_mode") and ADMIN_ID_INT is not None:
        soid = context.user_data.get("support_order_id", "?")
        await context.bot.send_message(
            chat_id=ADMIN_ID_INT,
            text=f"💬 Сообщение клиента (обращение №{soid})\nUser ID: {uid}\n\n{user_text}\n\nОтветить: /reply {soid} текст",
        )
        await update.message.reply_text("✅ Передал менеджеру. Ожидайте ответ.", reply_markup=main_menu_keyboard())
        return

    # ---------------- CATALOG SELECTION ----------------
    product_key = product_key_from_title(user_text)
    if product_key:
        promo_default = context.user_data.get("promo_default", "")
        product = PRODUCTS[product_key]
        if product.get("type") == "ready":
            oid = new_order_id()
            price = int(product.get("price", 0))
            price2, pct = apply_promo(price, promo_default) if promo_default else (price, 0)

            ORDERS_DB["orders"][str(oid)] = {
                "status": "priced",
                "user_id": uid,
                "user_label": user_label(update),
                "product": product_key,
                "product_title": product.get("title"),
                "price": price2,
                "promo": promo_default,
                "promo_pct": pct,
                "details": "ready_product",
                "created_at": now_iso(),
                "updated_at": now_iso(),
            }
            save_orders()

            pay = get_doc_by_name("payment.txt") or MBANK_REKV_FALLBACK or "💳 Оплата через MBank: 📌 +996999888332 Имронбек С."
            promo_line = f"\n🎟 Промокод: {promo_default} (-{pct}%)" if promo_default and pct else ""
            await update.message.reply_text(
                f"✅ Вы выбрали: {product['title']}\n"
                f"№{oid}\n"
                f"💰 К оплате: {price2} сом{promo_line}\n\n"
                f"{pay}\n\n"
                f"После оплаты нажмите «💳 Я оплатил(а) №{oid}» и отправьте чек.",
                reply_markup=payment_keyboard_for_order(str(oid)),
            )
            return

        # individual -> form
        await form_start(update, context, product_key)
        if promo_default:
            await update.message.reply_text(f"🎟 У вас активен промокод: {promo_default} (учту в цене).")
        return

    # payment button
    m = re.match(r"^💳\s*Я оплатил\(а\)\s*№(\d+)\s*$", user_text)
    if m:
        oid = m.group(1)
        order = ORDERS_DB.get("orders", {}).get(oid)
        if not order or order.get("user_id") != uid:
            await update.message.reply_text("❌ Заказ не найден. Откройте «📂 Каталог».", reply_markup=buy_menu_keyboard())
            return
        if order.get("status") != "priced":
            await update.message.reply_text(
                f"По заказу №{oid} сейчас статус: {order_status_human(order.get('status'))}.",
                reply_markup=main_menu_keyboard(),
            )
            return
        context.user_data["awaiting_receipt_order_id"] = oid
        await update.message.reply_text(f"🧾 Отлично! Отправьте чек (фото или PDF) по заказу №{oid}.", reply_markup=ReplyKeyboardRemove())
        return

    # default
    await update.message.reply_text(
        "Не понял 🤝\nОткройте «🛒 Покупка» → «📂 Каталог» или нажмите «🆘 Поддержка».",
        reply_markup=main_menu_keyboard(),
    )

# =====================================================
# MAIN
# =====================================================
def main():
    logging.basicConfig(level=logging.INFO)

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # user
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("broadcast", broadcast_cmd))  # admin

    # admin commands (optional)
    app.add_handler(CommandHandler("admin", admin_panel))
    app.add_handler(CommandHandler("pending", pending_cmd))
    app.add_handler(CommandHandler("setprice", setprice_cmd))
    app.add_handler(CommandHandler("reply", reply_cmd))
    app.add_handler(CommandHandler("confirm", confirm_cmd))
    app.add_handler(CommandHandler("reject", reject_cmd))
    app.add_handler(CommandHandler("inwork", inwork_cmd))
    app.add_handler(CommandHandler("ready", ready_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))

    # uploads
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))

    # text
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # reminder job
    if app.job_queue:
    app.job_queue.run_repeating(unpaid_reminder_job, interval=3600, first=3600)
else:
    logging.warning("JobQueue не установлен. Установи: python-telegram-bot[job-queue]")

    print("✅ Бот запущен")
    app.run_polling()

if __name__ == "__main__":
    main()
