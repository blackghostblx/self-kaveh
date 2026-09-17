from telethon.sync import TelegramClient
from telethon.sessions import StringSession

api_id = int(input("API_ID: "))
api_hash = input("API_HASH: ")

with TelegramClient(StringSession(), api_id, api_hash) as client:
    print("\nYour SESSION_STRING (copy this into Render env vars, keep it secret):\n")
    print(client.session.save())
