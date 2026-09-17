import asyncio
import os
from collections import OrderedDict

from aiohttp import web
from telethon import TelegramClient, events, functions
from telethon.sessions import StringSession

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from motor.motor_asyncio import AsyncIOMotorClient

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
MONGO_URI = os.environ["MONGO_URI"]  # required now — this is what makes multi-user persistent
PORT = int(os.environ.get("PORT", 8080))

db = AsyncIOMotorClient(MONGO_URI)["selfbot"]

# ---------------------------------------------------------------------------
# Per-user in-memory state
# ---------------------------------------------------------------------------
user_clients = {}    # uid -> TelegramClient (their own account)
user_settings = {}   # uid -> {"online": bool, "antidelete": bool, "style": str|None}
user_schedules = {}  # uid -> {sched_id: {chat_id, chat_label, interval, text, task}}
user_cache = {}       # uid -> OrderedDict[(chat_id, msg_id) -> Message]

MAX_CACHE = 1500


def default_settings():
    return {"online": False, "antidelete": False, "style": None}


# ---------------------------------------------------------------------------
# Fancy unicode fonts
# ---------------------------------------------------------------------------
def build_map(upper_start, lower_start, digit_start=None):
    m = {}
    for i in range(26):
        m[chr(65 + i)] = chr(upper_start + i)
        m[chr(97 + i)] = chr(lower_start + i)
    if digit_start is not None:
        for i in range(10):
            m[chr(48 + i)] = chr(digit_start + i)
    return m


FONT_MAPS = {
    "bold": build_map(0x1D400, 0x1D41A, 0x1D7CE),
    "italic": build_map(0x1D434, 0x1D44E),
    "bolditalic": build_map(0x1D468, 0x1D482),
    "mono": build_map(0x1D670, 0x1D68A, 0x1D7F6),
    "circled": build_map(0x24B6, 0x24D0),
}
FONT_LABELS = {
    "bold": "𝐁𝐨𝐥𝐝", "italic": "𝐼𝑡𝑎𝑙𝑖𝑐", "bolditalic": "𝑩𝒐𝒍𝒅𝑰𝒕𝒂𝒍𝒊𝒄",
    "mono": "𝙼𝚘𝚗𝚘", "circled": "Ⓒⓘⓡⓒⓛⓔⓓ",
}


def to_font(text, style):
    m = FONT_MAPS.get(style)
    if not m:
        return text
    return "".join(m.get(ch, ch) for ch in text)


# ---------------------------------------------------------------------------
# Mongo persistence (per user, keyed by their control-bot Telegram id)
# ---------------------------------------------------------------------------
async def save_user_session(uid, session_string):
    await db.users.update_one({"_id": uid}, {"$set": {"session": session_string}}, upsert=True)


async def delete_user_session(uid):
    await db.users.delete_one({"_id": uid})
    await db.settings.delete_one({"_id": uid})
    await db.schedules.delete_many({"uid": uid})


async def save_user_setting(uid, key, value):
    await db.settings.update_one({"_id": uid}, {"$set": {key: value}}, upsert=True)


async def save_schedule_doc(uid, sched_id, chat_id, chat_label, interval, text):
    await db.schedules.update_one(
        {"_id": f"{uid}:{sched_id}"},
        {"$set": {"uid": uid, "chat_id": chat_id, "chat_label": chat_label, "interval": interval, "text": text}},
        upsert=True,
    )


async def delete_schedule_doc(uid, sched_id):
    await db.schedules.delete_one({"_id": f"{uid}:{sched_id}"})


# ---------------------------------------------------------------------------
# Per-user Telethon client setup
# ---------------------------------------------------------------------------
async def register_client_handlers(uid, tclient):
    @tclient.on(events.NewMessage())
    async def _on_new_message(event):
        cache = user_cache.setdefault(uid, OrderedDict())
        key = (event.chat_id, event.id)
        cache[key] = event.message
        cache.move_to_end(key)
        if len(cache) > MAX_CACHE:
            cache.popitem(last=False)

        style = user_settings.get(uid, {}).get("style")
        if event.out and style:
            styled = to_font(event.raw_text or "", style)
            if styled and styled != event.raw_text:
                try:
                    await event.edit(styled)
                except Exception:
                    pass

    @tclient.on(events.MessageDeleted())
    async def _on_deleted(event):
        if not user_settings.get(uid, {}).get("antidelete"):
            return
        cache = user_cache.get(uid, {})
        for mid in event.deleted_ids:
            msg = cache.pop((event.chat_id, mid), None)
            if not msg:
                continue
            try:
                sender = await msg.get_sender()
                name = getattr(sender, "first_name", None) or getattr(sender, "title", None) or "Unknown"
            except Exception:
                name = "Unknown"
            caption = f"deleted message from {name}:\n\n{msg.text or ''}".strip()
            try:
                if msg.media:
                    await tclient.send_file("me", msg.media, caption=caption)
                else:
                    await tclient.send_message("me", caption)
            except Exception as e:
                print(f"[antidelete:{uid}] forward error: {e}")


async def connect_user(uid, session_string):
    """Try to connect a Telethon client for this user. Returns True on success."""
    tclient = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    await tclient.connect()
    if not await tclient.is_user_authorized():
        await tclient.disconnect()
        return False

    user_clients[uid] = tclient
    user_settings.setdefault(uid, default_settings())
    user_schedules.setdefault(uid, {})
    await register_client_handlers(uid, tclient)
    return True


def start_schedule(uid, sched_id, chat_id, chat_label, interval, text):
    async def loop():
        while True:
            try:
                tclient = user_clients.get(uid)
                if tclient:
                    await tclient.send_message(chat_id, text)
            except Exception as e:
                print(f"[schedule {uid}:{sched_id}] send error: {e}")
            await asyncio.sleep(interval)

    task = asyncio.create_task(loop())
    user_schedules.setdefault(uid, {})[sched_id] = {
        "chat_id": chat_id, "chat_label": chat_label,
        "interval": interval, "text": text, "task": task,
    }


def next_schedule_id(uid):
    existing = user_schedules.get(uid, {})
    n = 1
    while str(n) in existing:
        n += 1
    return str(n)


async def online_loop():
    while True:
        for uid, tclient in list(user_clients.items()):
            if user_settings.get(uid, {}).get("online"):
                try:
                    await tclient(functions.account.UpdateStatusRequest(offline=False))
                except Exception as e:
                    print(f"[online:{uid}] error: {e}")
        await asyncio.sleep(50)


async def load_all_users():
    async for doc in db.users.find({}):
        uid = doc["_id"]
        ok = await connect_user(uid, doc["session"])
        if not ok:
            print(f"[startup] session for {uid} is no longer valid, skipping")
            continue

        settings_doc = await db.settings.find_one({"_id": uid})
        if settings_doc:
            for k in ("online", "antidelete", "style"):
                if k in settings_doc:
                    user_settings[uid][k] = settings_doc[k]

        async for sched in db.schedules.find({"uid": uid}):
            sched_id = sched["_id"].split(":", 1)[1]
            start_schedule(uid, sched_id, sched["chat_id"], sched.get("chat_label", str(sched["chat_id"])),
                            sched["interval"], sched["text"])


# ---------------------------------------------------------------------------
# Control bot UI
# ---------------------------------------------------------------------------
CONNECT_SESSION, ASK_CHAT, ASK_INTERVAL, ASK_TEXT = range(4)

CONNECT_WARNING = (
    "این کد چیزیه که معادل رمز کامل ورود به اکانتته. هرکی این رو داشته باشه می‌تونه "
    "کامل جای تو وارد تلگرامت بشه. فقط اگه به این سرویس اعتماد داری ادامه بده.\n\n"
    "برای ساختنش، روی گوشی خودت (مثلاً با Termux) این رو اجرا کن:\n"
    "pip install telethon\n"
    "python generate_session.py\n\n"
    "بعد از لاگین با شماره و کدی که تلگرام برات میفرسته (و رمز دومرحله‌ای اگه داری)، "
    "یه رشته‌ی طولانی بهت میده. همون رو اینجا بفرست."
)


def connect_button_markup():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔗 اتصال اکانت", callback_data="connect_account")]])


def main_menu_markup(uid):
    s = user_settings.get(uid, default_settings())
    online_mark = "✅" if s["online"] else "❌"
    antidelete_mark = "✅" if s["antidelete"] else "❌"
    rows = [
        [InlineKeyboardButton(f"آنلاین‌ساز {online_mark}", callback_data="toggle_online")],
        [InlineKeyboardButton(f"آنتی‌دیلیت {antidelete_mark}", callback_data="toggle_antidelete")],
        [InlineKeyboardButton("🔤 فونت پیام‌ها", callback_data="menu_font")],
        [InlineKeyboardButton("⏱ زمان‌بندی پیام‌ها", callback_data="menu_schedule")],
        [InlineKeyboardButton("🔌 قطع اتصال اکانت", callback_data="disconnect_account")],
    ]
    return InlineKeyboardMarkup(rows)


def font_menu_markup(uid):
    s = user_settings.get(uid, default_settings())
    rows = []
    for key, label in FONT_LABELS.items():
        mark = " ✅" if s["style"] == key else ""
        rows.append([InlineKeyboardButton(label + mark, callback_data=f"font_{key}")])
    off_mark = " ✅" if not s["style"] else ""
    rows.append([InlineKeyboardButton("خاموش" + off_mark, callback_data="font_off")])
    rows.append([InlineKeyboardButton("⬅️ بازگشت", callback_data="menu_main")])
    return InlineKeyboardMarkup(rows)


def schedule_menu_markup(uid):
    rows = []
    for sid, s in user_schedules.get(uid, {}).items():
        label = f"❌ #{sid} | هر {s['interval']}ث | {s['chat_label']}"
        rows.append([InlineKeyboardButton(label, callback_data=f"delschedule_{sid}")])
    rows.append([InlineKeyboardButton("➕ افزودن زمان‌بندی جدید", callback_data="add_schedule")])
    rows.append([InlineKeyboardButton("⬅️ بازگشت", callback_data="menu_main")])
    return InlineKeyboardMarkup(rows)


def is_connected(uid):
    return uid in user_clients


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if is_connected(uid):
        await update.message.reply_text("پنل کنترل سلف 👇", reply_markup=main_menu_markup(uid))
    else:
        await update.message.reply_text(
            "سلام 👋 برای استفاده باید اول اکانت تلگرامت رو وصل کنی.",
            reply_markup=connect_button_markup(),
        )


async def connect_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(CONNECT_WARNING)
    return CONNECT_SESSION


async def connect_receive_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    session_string = update.message.text.strip()
    msg = await update.message.reply_text("در حال بررسی...")

    try:
        ok = await connect_user(uid, session_string)
    except Exception as e:
        ok = False
        print(f"[connect:{uid}] error: {e}")

    if not ok:
        await msg.edit_text("این رشته معتبر نیست یا منقضی شده. دوباره امتحان کن، یا /cancel بزن.")
        return CONNECT_SESSION

    await save_user_session(uid, session_string)
    await msg.edit_text("اکانتت وصل شد ✅")
    await update.message.reply_text("پنل کنترل سلف 👇", reply_markup=main_menu_markup(uid))
    return ConversationHandler.END


async def cancel_conv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("لغو شد.")
    return ConversationHandler.END


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    q = update.callback_query

    if not is_connected(uid) and q.data != "connect_account":
        await q.answer("اول باید اکانتت رو وصل کنی.", show_alert=True)
        return
    await q.answer()
    data = q.data

    if data == "menu_main":
        await q.edit_message_text("پنل کنترل سلف 👇", reply_markup=main_menu_markup(uid))

    elif data == "toggle_online":
        user_settings[uid]["online"] = not user_settings[uid]["online"]
        await save_user_setting(uid, "online", user_settings[uid]["online"])
        await q.edit_message_text("پنل کنترل سلف 👇", reply_markup=main_menu_markup(uid))

    elif data == "toggle_antidelete":
        user_settings[uid]["antidelete"] = not user_settings[uid]["antidelete"]
        await save_user_setting(uid, "antidelete", user_settings[uid]["antidelete"])
        await q.edit_message_text("پنل کنترل سلف 👇", reply_markup=main_menu_markup(uid))

    elif data == "menu_font":
        await q.edit_message_text("یه فونت انتخاب کن:", reply_markup=font_menu_markup(uid))

    elif data.startswith("font_"):
        val = data[len("font_"):]
        user_settings[uid]["style"] = None if val == "off" else val
        await save_user_setting(uid, "style", user_settings[uid]["style"])
        await q.edit_message_text("یه فونت انتخاب کن:", reply_markup=font_menu_markup(uid))

    elif data == "menu_schedule":
        await q.edit_message_text("زمان‌بندی‌های فعال (برای حذف لمس کن):", reply_markup=schedule_menu_markup(uid))

    elif data.startswith("delschedule_"):
        sid = data[len("delschedule_"):]
        entry = user_schedules.get(uid, {}).pop(sid, None)
        if entry:
            entry["task"].cancel()
            await delete_schedule_doc(uid, sid)
        await q.edit_message_text("زمان‌بندی‌های فعال (برای حذف لمس کن):", reply_markup=schedule_menu_markup(uid))

    elif data == "disconnect_account":
        tclient = user_clients.pop(uid, None)
        if tclient:
            await tclient.disconnect()
        for entry in user_schedules.pop(uid, {}).values():
            entry["task"].cancel()
        user_settings.pop(uid, None)
        user_cache.pop(uid, None)
        await delete_user_session(uid)
        await q.edit_message_text("اکانتت قطع شد. برای اتصال دوباره /start رو بزن.")


async def add_schedule_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = update.effective_user.id
    if not is_connected(uid):
        await q.answer("اول باید اکانتت رو وصل کنی.", show_alert=True)
        return ConversationHandler.END
    await q.answer()
    await q.edit_message_text(
        "لینک یا یوزرنیم چتی که می‌خوای پیام بره رو بفرست.\n"
        "مثلاً @channel یا https://t.me/channel — برای سیومسیج خودت بنویس me"
    )
    return ASK_CHAT


async def add_schedule_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    tclient = user_clients[uid]
    raw = update.message.text.strip()

    if raw.lower() == "me":
        context.user_data["sched_chat_id"] = "me"
        context.user_data["sched_chat_label"] = "me"
    else:
        target = raw.replace("https://t.me/", "").replace("http://t.me/", "").lstrip("@")
        try:
            entity = await tclient.get_entity(target)
        except Exception:
            await update.message.reply_text("پیدا نشد. یه لینک/یوزرنیم معتبر بفرست یا me رو بفرست برای سیومسیج:")
            return ASK_CHAT
        context.user_data["sched_chat_id"] = entity.id
        context.user_data["sched_chat_label"] = getattr(entity, "title", None) or getattr(entity, "username", None) or str(entity.id)

    await update.message.reply_text("هر چند ثانیه ارسال بشه؟ (مثلاً 180)")
    return ASK_INTERVAL


async def add_schedule_interval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text.strip()
    if not raw.isdigit() or int(raw) < 5:
        await update.message.reply_text("یه عدد معتبر بفرست (حداقل 5 ثانیه):")
        return ASK_INTERVAL
    context.user_data["sched_interval"] = int(raw)
    await update.message.reply_text("متن پیامی که باید فرستاده بشه رو بفرست:")
    return ASK_TEXT


async def add_schedule_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = update.message.text
    sid = next_schedule_id(uid)
    chat_id = context.user_data["sched_chat_id"]
    chat_label = context.user_data["sched_chat_label"]
    interval = context.user_data["sched_interval"]

    start_schedule(uid, sid, chat_id, chat_label, interval, text)
    await save_schedule_doc(uid, sid, chat_id, chat_label, interval, text)

    await update.message.reply_text(f"زمان‌بندی #{sid} ثبت شد: هر {interval} ثانیه به {chat_label}")
    await update.message.reply_text("پنل کنترل سلف 👇", reply_markup=main_menu_markup(uid))
    return ConversationHandler.END


def build_control_bot():
    app = Application.builder().token(BOT_TOKEN).build()

    connect_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(connect_entry, pattern="^connect_account$")],
        states={CONNECT_SESSION: [MessageHandler(filters.TEXT & ~filters.COMMAND, connect_receive_session)]},
        fallbacks=[CommandHandler("cancel", cancel_conv)],
    )

    schedule_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(add_schedule_entry, pattern="^add_schedule$")],
        states={
            ASK_CHAT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_schedule_chat)],
            ASK_INTERVAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_schedule_interval)],
            ASK_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_schedule_text)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conv)],
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(connect_conv)
    app.add_handler(schedule_conv)
    app.add_handler(CallbackQueryHandler(on_callback))
    return app


# ---------------------------------------------------------------------------
# Tiny web server so Render's health check is happy
# ---------------------------------------------------------------------------
async def start_web():
    web_app = web.Application()
    web_app.router.add_get("/", lambda r: web.Response(text="self-bot service is running"))
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main():
    await start_web()
    await load_all_users()
    asyncio.create_task(online_loop())

    control_app = build_control_bot()
    await control_app.initialize()
    await control_app.start()
    await control_app.updater.start_polling()

    print("control bot running.")
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await control_app.updater.stop()
        await control_app.stop()
        await control_app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

