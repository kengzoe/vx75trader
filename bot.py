# Fix for Python 3.14 idna encoding
import encodings.idna

import os
import json
import logging
import threading
import time
import numpy as np
from datetime import datetime, timezone
from collections import deque
from flask import Flask, request
import websocket
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ========== CONFIG ==========
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DERIV_TOKEN = os.getenv("DERIV_TOKEN")
RENDER_URL = os.getenv("RENDER_URL", "https://vx75trader.onrender.com")
DERIV_APP_ID = "1089"
SYMBOL = "R_75"
STAKE = float(os.getenv("STAKE", "0.35"))
MAX_DAILY_TRADES = int(os.getenv("MAX_DAILY_TRADES", "5"))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "50"))
USE_DEMO = os.getenv("USE_DEMO", "true").lower() == "true"

CHAT_ID = None
RUNNING = False

# ========== STATE ==========
ticks = deque(maxlen=500)
candles = deque(maxlen=50)
current_price = 0
daily_trades = 0
daily_pnl = 0
active_trade = None
last_trade_time = 0
cooldown_seconds = 300

# ========== FLASK ==========
app = Flask(__name__)

@app.route('/')
def home():
    return "VX75 Trader is running!"

# ========== DERIV WEBSOCKET ==========
ws = None
ws_connected = False

def send_deriv(data):
    if ws and ws_connected:
        try:
            ws.send(json.dumps(data))
        except Exception as e:
            logger.error(f"Send error: {e}")

def on_message(ws_app, message):
    global current_price, candles, active_trade, daily_pnl, daily_trades
    
    try:
        data = json.loads(message)
    except:
        return
    
    # Handle errors
    if "error" in data:
        logger.error(f"Deriv error: {data['error']}")
        return
    
    # Handle auth response
    if "authorize" in data:
        auth = data["authorize"]
        logger.info(f"Auth: {auth.get('loginid', 'unknown')} | Balance: {auth.get('balance', 'N/A')}")
        # Now subscribe to ticks
        send_deriv({"ticks": SYMBOL, "subscribe": 1})
        return
    
    # Handle ticks
    if "tick" in data:
        tick = data["tick"]
        price = float(tick["quote"])
        current_price = price
        ticks.append({"price": price, "time": time.time()})
        build_candles(price)
        
        if RUNNING:
            if active_trade:
                pass
            else:
                check_entry_signal()
    
    # Handle trade opened
    if "buy" in data and "contract_id" in data["buy"]:
        active_trade = {
            "id": data["buy"]["contract_id"],
            "entry": current_price,
            "time": time.time()
        }
        logger.info(f"Trade opened: {data['buy']['contract_id']}")
    
    # Handle trade closed
    if "proposal_open_contract" in data:
        poc = data["proposal_open_contract"]
        if poc.get("is_sold"):
            pnl = float(poc.get("profit", 0))
            daily_pnl += pnl
            daily_trades += 1
            active_trade = None
            logger.info(f"Trade closed. PnL: ${pnl:.2f}")
            
            if CHAT_ID:
                emoji = "🟢" if pnl > 0 else "🔴"
                msg = f"{emoji} Trade Closed\nPnL: ${pnl:.2f}\nDaily: ${daily_pnl:.2f}\nTrades: {daily_trades}/{MAX_DAILY_TRADES}"
                try:
                    import asyncio
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(send_telegram(msg))
                except:
                    pass

def on_error(ws_app, error):
    logger.error(f"WS Error: {error}")

def on_close(ws_app, status, msg):
    global ws_connected
    ws_connected = False
    logger.info(f"Disconnected. Status: {status}, Msg: {msg}. Reconnecting...")
    time.sleep(5)
    connect_deriv()

def on_open(ws_app):
    global ws_connected
    ws_connected = True
    logger.info("Connected to Deriv")
    # Skip auth - just subscribe to ticks (works without token for prices)
    send_deriv({"ticks": SYMBOL, "subscribe": 1})

def connect_deriv():
    global ws
    ws = websocket.WebSocketApp(
        f"wss://ws.binaryws.com/websockets/v3?app_id={DERIV_APP_ID}",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    ws.run_forever(ping_interval=30, ping_timeout=10)

# ========== CANDLE BUILDING ==========
current_candle = None

def build_candles(price):
    global current_candle
    now = datetime.now(timezone.utc)
    minute_key = now.strftime("%Y-%m-%d %H:%M")
    
    if current_candle and current_candle["key"] == minute_key:
        current_candle["high"] = max(current_candle["high"], price)
        current_candle["low"] = min(current_candle["low"], price)
        current_candle["close"] = price
    else:
        if current_candle:
            candles.append(current_candle)
        current_candle = {
            "key": minute_key,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "time": now
        }

# ========== INDICATORS ==========
def calculate_bollinger(prices, period=20, std_dev=2):
    if len(prices) < period:
        return None, None, None
    recent = list(prices)[-period:]
    sma = np.mean(recent)
    std = np.std(recent)
    return sma + (std_dev * std), sma, sma - (std_dev * std)

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50
    recent = list(prices)[-period-1:]
    gains, losses = [], []
    for i in range(1, len(recent)):
        diff = recent[i] - recent[i-1]
        gains.append(diff if diff > 0 else 0)
        losses.append(abs(diff) if diff < 0 else 0)
    avg_gain = np.mean(gains) if gains else 0
    avg_loss = np.mean(losses) if losses else 0
    if avg_loss == 0:
        return 100
    return 100 - (100 / (1 + avg_gain / avg_loss))

def detect_volume_spike():
    if len(ticks) < 50:
        return False
    now = time.time()
    recent = sum(1 for t in ticks if now - t["time"] < 10)
    avg = len([t for t in ticks if now - t["time"] < 60]) / 6
    return recent > avg * 2 if avg > 0 else False

# ========== TRADING LOGIC ==========
def check_entry_signal():
    global active_trade, last_trade_time, daily_trades, daily_pnl
    
    if active_trade:
        return
    if daily_trades >= MAX_DAILY_TRADES:
        return
    if daily_pnl <= -MAX_DAILY_LOSS:
        return
    if time.time() - last_trade_time < cooldown_seconds:
        return
    if len(candles) < 20:
        return
    
    prices = [c["close"] for c in candles]
    current = prices[-1]
    
    upper_bb, mid_bb, lower_bb = calculate_bollinger(prices)
    if upper_bb is None:
        return
    
    rsi = calculate_rsi(prices)
    volume_spike = detect_volume_spike()
    
    trade_type = None
    
    if current <= lower_bb and rsi < 30 and volume_spike:
        trade_type = "CALL"
    elif current >= upper_bb and rsi > 70 and volume_spike:
        trade_type = "PUT"
    
    if trade_type:
        execute_trade(trade_type)

def execute_trade(trade_type):
    global last_trade_time
    
    send_deriv({
        "buy": "1",
        "price": str(STAKE),
        "parameters": {
            "contract_type": trade_type,
            "amount": str(STAKE),
            "currency": "USD",
            "duration": 5,
            "duration_unit": "t",
            "symbol": SYMBOL
        }
    })
    
    last_trade_time = time.time()
    
    if CHAT_ID:
        emoji = "🟢" if trade_type == "CALL" else "🔴"
        msg = f"{emoji} VX75 {trade_type}\nEntry: {current_price:.1f}\nStake: ${STAKE}\nDuration: 5 ticks"
        try:
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(send_telegram(msg))
        except:
            pass

# ========== TELEGRAM ==========
async def send_telegram(msg):
    if CHAT_ID:
        bot = ApplicationBuilder().token(TELEGRAM_TOKEN).build().bot
        await bot.send_message(chat_id=CHAT_ID, text=msg)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global CHAT_ID
    CHAT_ID = update.effective_chat.id
    await update.message.reply_text(
        "🟣 VX75 AUTO-TRADER\n\n"
        f"Mode: {'DEMO' if USE_DEMO else 'LIVE'}\n"
        f"Stake: ${STAKE}\n"
        f"Max trades/day: {MAX_DAILY_TRADES}\n"
        f"Max loss: ${MAX_DAILY_LOSS}\n\n"
        "/start_bot - Start trading\n"
        "/stop_bot - Stop\n"
        "/status - Check state\n"
        "/set_stake 0.50 - Change stake"
    )

async def start_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global RUNNING, CHAT_ID
    CHAT_ID = update.effective_chat.id
    if RUNNING:
        await update.message.reply_text("Already running")
        return
    RUNNING = True
    await update.message.reply_text("🚀 Trading activated!")

async def stop_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global RUNNING
    RUNNING = False
    await update.message.reply_text("⏸️ Stopped")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global current_price, daily_trades, daily_pnl, active_trade, RUNNING
    await update.message.reply_text(
        f"📊 VX75 STATUS\n\n"
        f"Mode: {'DEMO' if USE_DEMO else 'LIVE'}\n"
        f"Running: {'✅' if RUNNING else '❌'}\n"
        f"Price: {current_price:.1f}\n"
        f"Active trade: {'Yes' if active_trade else 'No'}\n"
        f"Trades: {daily_trades}/{MAX_DAILY_TRADES}\n"
        f"PnL: ${daily_pnl:.2f}\n"
        f"Limit: ${MAX_DAILY_LOSS}"
    )

async def set_stake(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global STAKE
    if not context.args:
        await update.message.reply_text("/set_stake 0.50")
        return
    STAKE = float(context.args[0])
    await update.message.reply_text(f"✅ Stake: ${STAKE}")

# ========== APPLICATION ==========
application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
application.add_handler(CommandHandler("start", start))
application.add_handler(CommandHandler("start_bot", start_bot))
application.add_handler(CommandHandler("stop_bot", stop_bot))
application.add_handler(CommandHandler("status", status))
application.add_handler(CommandHandler("set_stake", set_stake))

# ========== WEBHOOK ==========
async def init_bot():
    await application.initialize()
    await application.bot.set_webhook(url=f"{RENDER_URL}/webhook")
    logger.info("Webhook set!")

@app.route('/webhook', methods=['POST'])
def webhook():
    import asyncio
    update = Update.de_json(request.get_json(force=True), application.bot)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(application.initialize())
    loop.run_until_complete(application.process_update(update))
    return "ok"

def run_init():
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(init_bot())

# ========== START ==========
threading.Thread(target=run_init, daemon=True).start()
threading.Thread(target=connect_deriv, daemon=True).start()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
