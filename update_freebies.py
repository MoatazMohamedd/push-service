import os
import json
import re
import string
import requests
from datetime import datetime, timezone
from google.cloud import firestore
from google.oauth2 import service_account
import firebase_admin
from firebase_admin import messaging
from difflib import SequenceMatcher

# -----------------
# ENV VARIABLES
# -----------------
FCM_TOPIC = "free_games"
GAMERPOWER_API = "https://www.gamerpower.com/api/filter?platform=epic-games-store.steam.gog.origin&type=game&sort-by=date"
LOCAL_JSON_FILE = "freebies.json"

IGDB_CLIENT_ID = os.getenv("IGDB_CLIENT_ID")
IGDB_ACCESS_TOKEN = os.getenv("IGDB_ACCESS_TOKEN")

FIREBASE_CREDENTIALS_JSON = os.getenv("FIREBASE_CREDENTIALS_JSON")
FIRESTORE_PROJECT_ID = os.getenv("FIRESTORE_PROJECT_ID")

# -----------------
# FIREBASE SETUP
# -----------------
firebase_cred_dict = json.loads(FIREBASE_CREDENTIALS_JSON)
firebase_admin.initialize_app(firebase_admin.credentials.Certificate(firebase_cred_dict))

# -----------------
# FIRESTORE CLIENT
# -----------------
credentials = service_account.Credentials.from_service_account_info(firebase_cred_dict)
firestore_client = firestore.Client(project=FIRESTORE_PROJECT_ID, credentials=credentials)

# -----------------
# HELPERS
# -----------------
def normalize_title(title):
    title = title.lower()
    title = re.sub(rf"[{re.escape(string.punctuation)}]", "", title)
    title = re.sub(r"\s+", " ", title)
    return title.strip()


def fetch_gamerpower_games():
    try:
        resp = requests.get(GAMERPOWER_API, timeout=10)
        resp.raise_for_status()
        offers = resp.json()

        games = []
        for offer in offers:
            if "Key Giveaway" in offer["title"]:
                continue

            clean_title = re.sub(r"\s*\(.*?\)", "", offer["title"])
            clean_title = re.sub(r"\s*Giveaway", "", clean_title).strip()

            # Store detection
            store = "Unknown"
            if "Steam" in offer.get("platforms", ""):
                store = "Steam"
            elif "Epic Games" in offer.get("platforms", ""):
                store = "Epic Games Store"
            elif "GoG" in offer.get("platforms", ""):
                store = "GoG"
            elif "Origin" in offer.get("platforms", ""):
                store = "Origin"

            worth = offer.get("worth", "$0.00").replace("$", "").strip() or "0.00"

            games.append({
                "gamerpower_id": offer["id"],
                "title": clean_title,
                "worth": worth,
                "store": store,
                "expiry_date": offer.get("end_date", "N/A"),
                "new_notified": False,
                "urgent_notified": False
            })
        return games
    except Exception as e:
        print(f"Error fetching GamerPower data: {e}")
        return []


def similar(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def read_local_json(file_path=LOCAL_JSON_FILE):
    if not os.path.exists(file_path):
        return []

    with open(file_path, "r", encoding="utf-8") as f:
        try:
            data = f.read().strip()
            if not data:
                return []
            return json.loads(data)
        except json.JSONDecodeError:
            return []


def write_local_json(data):
    with open(LOCAL_JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def parse_expiry(expiry_str):
    if not expiry_str or expiry_str == "N/A":
        return None
    try:
        dt = datetime.strptime(expiry_str, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        print(f"[WARN] Could not parse expiry date: {expiry_str}")
        return None


def send_fcm_notification(game, urgent=False):
    if urgent:
        title = "⚡ LAST CHANCE: Free Game Ending Soon!"
        body = f"{game['title']} is FREE on {game['store']} but HURRY—offer ends within 24h!"
    else:
        title = "FREE GAME ALERT 🎮"
        body = f"{game['title']} is now FREE on {game['store']}!"

    message = messaging.Message(
        topic=FCM_TOPIC,
        notification=messaging.Notification(
            title=title,
            body=body
        ),
        data={
            "game_name": game["title"],
            "worth": game["worth"],
            "store": game["store"],
            "expiry_date": game["expiry_date"],
            "click_action": "OPEN_GAME_PAGE"
        }
    )
    try:
        messaging.send(message)
        print(f"Notification sent for {game['title']} (urgent={urgent})")
    except Exception as e:
        print(f"Notification failed for {game['title']}: {e}")


# -----------------
# MAIN LOGIC
# -----------------
def main():
    print("Fetching GamerPower freebies...")
    gp_games = fetch_gamerpower_games()
    old_list = read_local_json()

    old_dict = {g["gamerpower_id"]: g for g in old_list}
    enriched_games = []
    now = datetime.now(timezone.utc)
    updated = False

    for gp_game in gp_games:
        old_game = old_dict.get(gp_game["gamerpower_id"])
        expiry_dt = parse_expiry(gp_game.get("expiry_date"))

        # inherit old flags if present
        if old_game:
            gp_game["new_notified"] = old_game.get("new_notified", False)
            gp_game["urgent_notified"] = old_game.get("urgent_notified", False)

        # NEW GAME ALERT
        if not gp_game["new_notified"]:
            send_fcm_notification(gp_game, urgent=False)
            gp_game["new_notified"] = True
            updated = True

        # EXPIRY <24h ALERT
        if expiry_dt:
            hours_left = (expiry_dt - now).total_seconds() / 3600
            print(f"[CHECK] {gp_game['title']} expires in {hours_left:.2f}h")
            if hours_left <= 24 and not gp_game["urgent_notified"]:
                send_fcm_notification(gp_game, urgent=True)
                gp_game["urgent_notified"] = True
                updated = True

        enriched_games.append(gp_game)

    if updated or gp_games != old_list:
        firestore_client.collection("freebies").document("games").set({
            "games": enriched_games
        })
        write_local_json(enriched_games)
        print("Database & local JSON updated.")
    else:
        print("No new freebies or urgent expiries.")


if __name__ == "__main__":
    main()
