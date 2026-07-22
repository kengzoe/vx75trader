import os
import json
import logging
import threading
import time
import numpy as np
from datetime import datetime, timezone
from collections import deque
from flask import Flask
import websocket
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ========== CONFIG ==========
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DERIV_TOKEN = os.getenv("DERIV_TOKEN")
DERIV_APP_ID = "1089"
SYMBOL = "R_100"  # VX75 on Deriv
STAKE = float(os.getenv("STAKE", "0.35"))  # Minimum stake
MAX_DAILY_TRADES = int(os.getenv("MAX_DAILY_TRADES", "5"))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "50"))  # In account currency
USE_DEMO = os.getenv("USE_DEMO", "true").lower() == "true"

CHAT_ID = None
RUNNING = False

# ========== STATE ==========
ticks = deque(maxlen=1000)
candles = deque(maxlen=100)
current_price = 0
daily_trades = 0
daily_pnl = 0
active_trade = None
last_trade_time = 0
cooldown_seconds = 300  # 5 min between trades

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
        ws.send(json.dumps(data))

def on_message(ws_app, message):
    global current_price, candles, active_trade, daily_pnl, daily_trades
    
    data = json.loads(message)
    
    # Handle ticks
    if "tick" in data:
        tick = data["tick"]
        price = float(tick["quote"])
        current_price = price
        ticks.append({"price": price, "time": time.time()})
        
        # Build 1-min candles
        build_candles(price)
        
        # Check active trade
        if active_trade:
            check_trade_exit(price)
        else:
            check_entry_signal()
    
    # Handle trade confirmations
    if "buy" in data:
        buy_data = data["buy"]
        if "contract_id" in buy_data:
            active_trade = {
                "id": buy_data["contract_id"],
                "entry": current_price,
                "type": buy_data.get("longcode", ""),
                "purchase_time": time.time()
            }
            logger.info(f"Trade opened: {buy_data['contract_id']}")
    
    # Handle profit/loss
    if "proposal_open_contract" in data:
        poc = data["proposal_open_contract"]
        if "is_sold" in poc and poc["is_sold"]:
            pnl = float(poc.get("profit", 0))
            daily_pnl += pnl
            daily_trades += 1
            logger.info(f"Trade closed. PnL: ${pnl:.2f}")
            
            # Notify
            import asyncio
            if CHAT_ID:
                emoji = "🟢" if pnl > 0 else "🔴"
                msg = f"{emoji} Trade Closed\nPnL: ${pnl:.2f}\nDaily PnL: ${daily_pnl:.2f}\nTrades today: {daily_trades}"
                asyncio.run(send_telegram(msg))

def on_error(ws_app, error):
    logger.error(f"WebSocket error: {error}")

def on_close(ws_app, status, msg):
    global ws_connected
    ws_connected = False
    logger.info(f"Disconnected. Reconnecting in 5s...")
    time.sleep(5)
    connect_deriv()

def on_open(ws_app):
    global ws_connected
    ws_connected = True
    logger.info("Connected to Deriv")
    
    # Authorize
    send_deriv({"authorize": DERIV_TOKEN})
    time.sleep(1)
    
    # Subscribe to ticks
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
    ws.run_forever()

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
    
    upper = sma + (std_dev * std)
    lower = sma - (std_dev * std)
    
    return upper, sma, lower

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50
    
    recent = list(prices)[-period-1:]
    gains = []
    losses = []
    
    for i in range(1, len(recent)):
        diff = recent[i] - recent[i-1]
        if diff > 0:
            gains.append(diff)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(diff))
    
    avg_gain = np.mean(gains) if gains else 0
    avg_loss = np.mean(losses) if losses else 0
    
    if avg_loss == 0:
        return 100
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def detect_volume_spike():
    """Check if recent ticks show unusual activity"""
    if len(ticks) < 50:
        return False
    
    # Count ticks in last 10 seconds vs average
    now = time.time()
    recent_ticks = sum(1 for t in ticks if now - t["time"] < 10)
    avg_ticks = len([t for t in ticks if now - t["time"] < 60]) / 6  # per 10 seconds
    
    return recent_ticks > avg_ticks * 2

# ========== TRADING LOGIC ==========
def check_entry_signal():
    global active_trade, last_trade_time, daily_trades, daily_pnl
    
    # Safety checks
    if active_trade:
        return
    
    if daily_trades >= MAX_DAILY_TRADES:
        return
    
    if daily_pnl <= -MAX_DAILY_LOSS:
        logger.info("Daily loss limit reached")
        return
    
    now = time.time()
    if now - last_trade_time < cooldown_seconds:
        return
    
    if len(candles) < 20:
        return
    
    prices = [c["close"] for c in candles]
    current = prices[-1]
    
    # Calculate indicators
    upper_bb, mid_bb, lower_bb = calculate_bollinger(prices)
    rsi = calculate_rsi(prices)
    volume_spike = detect_volume_spike()
    
    if upper_bb is None:
        return
    
    # Entry signals
    trade_type = None
    
    # BUY: Oversold at lower band + RSI < 30 + volume spike
    if current <= lower_bb and rsi < 30 and volume_spike:
        trade_type = "CALL"
        logger.info(f"BUY signal: Price={current:.1f}, Lower BB={lower_bb:.1f}, RSI={rsi:.0f}")
    
    # SELL: Overbought at upper band + RSI > 70 + volume spike
    elif current >= upper_bb and rsi > 70 and volume_spike:
        trade_type = "PUT"
        logger.info(f"SELL signal: Price={current:.1f}, Upper BB={upper_bb:.1f}, RSI={rsi:.0f}")
    
    if trade_type:
        execute_trade(trade_type)

def execute_trade(trade_type):
    global last_trade_time, active_trade
    
    # 5-tick duration for VX75 (fast trades)
    duration = 5
    duration_unit = "t"
    
    # Build proposal request
    proposal = {
        "proposal": 1,
        "amount": str(STAKE),
        "basis": "stake",
        "contract_type": trade_type,
        "currency": "USD",
        "duration": duration,
        "duration_unit": duration_unit,
        "symbol": SYMBOL
    }
    
    send_deriv(proposal)
    
    # After proposal, buy immediately
    def buy_after_proposal():
        time.sleep(1)
        send_deriv({
            "buy": "1",
            "price": str(STAKE),
            "parameters": {
                "contract_type": trade_type,
                "amount": str(STAKE),
                "currency": "USD",
                "duration": duration,
                "duration_unit": duration_unit,
                "symbol": SYMBOL
            }
        })
    
    threading.Thread(target=buy_after_proposal).start()
    last_trade_time = time.time()
    
    # Notify
    import asyncio
    if CHAT_ID:
        emoji = "🟢" if trade_type == "CALL" else "🔴"
        msg = f"{emoji} VX75 {trade_type}\nEntry: {current_price:.1f}\nStake: ${STAKE}\nDuration: {duration} ticks"
        asyncio.run(send_telegram(msg))

def check_trade_exit(price):
    """Deriv contracts auto-close at expiry, but we track them"""
    pass  # Deriv handles exits automatically for tick contracts

# ========== TELEGRAM COMMANDS ==========
async def send_telegram(msg):
    global CHAT_ID
    if CHAT_ID:
        application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        await application.bot.send_message(chat_id=CHAT_ID, text=msg)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global CHAT_ID
    CHAT_ID = update.effective_chat.id
    await update.message.reply_text(
        "🟣 VX75 AUTO-TRADER\n\n"
        f"Mode: {'📊 DEMO' if USE_DEMO else '💰 LIVE'}\n"
        f"Stake: ${STAKE}\n"
        f"Max daily trades: {MAX_DAILY_TRADES}\n"
        f"Max daily loss: ${MAX_DAILY_LOSS}\n\n"
        "/start_bot - Activate trading\n"
        "/stop_bot - Stop trading\n"
        "/status - Current state\n"
        "/set_stake 0.50 - Change stake"
    )

async def start_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global RUNNING, CHAT_ID
    CHAT_ID = update.effective_chat.id
    if RUNNING:
        await update.message.reply_text("Already running")
        return
    RUNNING = True
    await update.message.reply_text("🚀 VX75 Auto-trader activated!")

async def stop_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global RUNNING
    RUNNING = False
    await update.message.reply_text("⏸️ Stopped")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global RUNNING, current_price, daily_trades, daily_pnl, active_trade
    await update.message.reply_text(
        f"📊 VX75 STATUS\n\n"
        f"Mode: {'DEMO' if USE_DEMO else 'LIVE'}\n"
        f"Running: {'✅' if RUNNING else '❌'}\n"
        f"Price: {current_price:.1f}\n"
        f"Active trade: {'Yes' if active_trade else 'No'}\n"
        f"Trades today: {daily_trades}/{MAX_DAILY_TRADES}\n"
        f"Daily PnL: ${daily_pnl:.2f}\n"
        f"Limit: ${MAX_DAILY_LOSS}"
    )

async def set_stake(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global STAKE
    if not context.args:
        await update.message.reply_text("/set_stake 0.50")
        return
    val = float(context.args[0])
    if val < 0.35:
        await update.message.reply_text("Min $0.35")
        return
    STAKE = val
    await update.message.reply_text(f"✅ Stake: ${STAKE}")

# ========== MAIN ==========
application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
application.add_handler(CommandHandler("start", start))
application.add_handler(CommandHandler("start_bot", start_bot))
application.add_handler(CommandHandler("stop_bot", stop_bot))
application.add_handler(CommandHandler("status", status))
application.add_handler(CommandHandler("set_stake", set_stake))

# Start Deriv connection
threading.Thread(target=connect_deriv, daemon=True).start()
