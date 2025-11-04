import os
import asyncio
from html import escape
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram.ext import Application, CommandHandler, MessageHandler, Updater, filters
from dotenv import load_dotenv
import sqlite3
import finnhub
from datetime import datetime

load_dotenv()

ADMIN_ID = [os.getenv("ADMIN_ID")]

def init_db():
    channel_chat_id = "@CanaryReports"
    conn = sqlite3.connect('news_bot.db')
    cursor = conn.cursor()

    # Table for Subscribed users
    cursor.execute('''CREATE TABLE IF NOT EXISTS subscribers (
        chat_id TEXT PRIMARY KEY 
        )
        ''')

    # HardCoded into CanaryReports Channel
    cursor.execute("INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)", (channel_chat_id,))
    conn.commit()

    # Prevent Duplicate Articles
    cursor.execute('''CREATE TABLE IF NOT EXISTS posted_articles (
    article_id TEXT PRIMARY KEY 
    )
    ''')

    conn.commit()
    conn.close()

init_db()

async def subscribe(update, context):
    if update.message.from_user.id not in ADMIN_ID:
        await update.message.reply_text("You are not licensed.")
        return
    chat_id = update.message.chat_id
    conn = sqlite3.connect('news_bot.db')
    cursor = conn.cursor()

    try:
        cursor.execute("INSERT INTO subscribers (chat_id) VALUES(?)", (chat_id,))
        conn.commit()
        await update.message.reply_text("You're Subscribed!")
    except sqlite3.IntegrityError:
        await update.message.reply_text("You're already Subscribed!")
    conn.close()


async def start(update, context):
    await update.message.reply_text("""
    Welcome to Canary Reports!
     /help - Opens this menu
     /subscribe - Subscribes to our automated news reports
     /unsubscribe - Unsubscribes from our automated news reports
    
     Made With Love By @CanaryReporter""")


async def help(update, context):
    await update.message.reply_text("""
    Welcome to Canary Reports!
     /help - Opens this menu
     /subscribe - Subscribes to our automated news reports
     /unsubscribe - Unsubscribes from our automated news reports
    
     Made With Love By @CanaryReporter""")


async def unsubscribe(update, context):
    chat_id = update.message.chat_id
    conn = sqlite3.connect('news_bot.db')
    cursor = conn.cursor()

    try:
        cursor.execute("DELETE FROM subscribers WHERE chat_id = ?", (chat_id,))
        conn.commit()
        await update.message.reply_text("You're Unsubscribed!")
    except sqlite3.IntegrityError:
        await update.message.reply_text("You are not Subscribed!")
    conn.close()


async def fetch_and_send_news(app):
    print(f"fetching news at {datetime.now()}")
    finnhub_client = finnhub.Client(api_key=os.environ['FINNHUB_API_KEY'])
    conn = sqlite3.connect('news_bot.db')
    cursor = conn.cursor()

    # snag the data from the dataset
    news = finnhub_client.general_news('general', min_id=0)

    # all the users from the subscriber db
    cursor.execute("SELECT chat_id FROM subscribers")
    subscribers = cursor.fetchall()

    financial_keywords = ['stock', 'market', 'trading', 'finance', 'economy', 'fed', 'wall street', 'nasdaq', 'dow',
                          'crypto', 'bitcoin', 'geopolitical', 'gaza', 'israel', 'cramer', 'jpmorgan', 'chase',
                          'bank of america', 'blackrock', 'vanguard', 'trump', 'china', 'usa', 'treasury', 'war',
                          'mastercard', 'visa', 'binance', 'bitget', 'kucoin', 's&p', 'nvidia', 'amd', 'microsoft',
                          'us', 'gold', 'xau', 'elon musk', 'tesla', 'solana', 'bnb', 'michael saylor', 'robinhood',
                          'ETF', 'openai', 'ethereum', 'amazon', 'tether', 'coinbase', 'white house', 'powell', 'polymarket',
                          'bybit', 'cpi', 'ppi', 'fomc', 'nfp', 'inflation', 'rate cut', 'housing', 'blockchain', 'interest rate', ]
    for article in news[:10]:  # send top 5 articles
        article_id = article['id']
        text = (escape(article['headline']) + ' ' + escape(article['summary'])).lower()
        if not any(keyword in text for keyword in financial_keywords):
            continue

        # check if already posted
        cursor.execute("SELECT article_id FROM posted_articles WHERE article_id = ?", (article_id,))
        if cursor.fetchone():
            continue

        # post it to all the subscribers
        for (chat_id,) in subscribers:
            try:
                message = f" <b>{escape(article['headline'])}</b>\n\n <i>{escape(article['summary'])}</i>\n\n Channel: @CanaryReports \n\n News: @CanaryReportsBot\n\n<a href=\"{article['url']}\"> Source</a>"
                await app.bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')
            except Exception as e:
                print(f"Failed to send to {chat_id}: {e}")

        # add it into db so never posted again
        cursor.execute("INSERT INTO posted_articles VALUES (?)", (article_id,))
        conn.commit()

    conn.close()

async def send_news(update, context):
    if update.message.from_user.id not in ADMIN_ID:
        await update.message.reply_text("You are not authorized to do that!")
        await update.message.reply_text("Buy a License.")
        return
    await fetch_and_send_news(context.application)
    await update.message.reply_text("Sent all subscribers news!")



async def main():
    app = Application.builder().token(os.environ['TELEGRAM_TOKEN']).build()
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('subscribe', subscribe))
    app.add_handler(CommandHandler('unsubscribe', unsubscribe))
    app.add_handler(CommandHandler('news', send_news))
    app.add_handler(CommandHandler('help', help))
    # Interval Sending
    scheduler = AsyncIOScheduler()
    scheduler.add_job(fetch_and_send_news, 'interval', seconds=30, args=[app])

    scheduler.start()

    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    await asyncio.Event().wait()
    print('Polling!')





if __name__ == '__main__':
    asyncio.run(main())