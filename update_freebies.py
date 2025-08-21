import os
import json
import re
import string
import requests
from datetime import datetime
from datetime import timezone
from google.cloud import firestore
from google.oauth2 import service_account
import firebase_admin
from firebase_admin import messaging
from difflib import SequenceMatcher
# -----------------
# ENV VARIABLES
# -----------------
FCM_TOPIC = "/topics/free_games"
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
                "expiry_date": offer.get("end_date", "N/A")
            })
        return games
    except Exception as e:
        print(f"Error fetching GamerPower data: {e}")
        return []

def similar(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def read_local_json(file_path="freebies.json"):
    if not os.path.exists(file_path):
        return []

    with open(file_path, "r", encoding="utf-8") as f:
        try:
            data = f.read().strip()
            if not data:  # empty file
                return []
            return json.loads(data)
        except json.JSONDecodeError:
            return []

def write_local_json(data):
    with open(LOCAL_JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def fetch_igdb_data(title):
    url = "https://api.igdb.com/v4/games"
    headers = {
        "Client-ID": IGDB_CLIENT_ID,
        "Authorization": f"Bearer {IGDB_ACCESS_TOKEN}",
    }

    body = f'''
    search "{title}";
    fields id, name, cover.url, total_rating, storyline, first_release_date,
           summary, genres.name, player_perspectives.name, game_engines.name,
           game_modes.name, screenshots.url, websites.url;
    limit 10;
    '''

    try:
        resp = requests.post(url, headers=headers, data=body.strip(), timeout=10)
        resp.raise_for_status()
        results = resp.json()

        if not results:
            return {}

        # Normalize the input title
        normalized_title = normalize_title(title)

        # Pick the best match based on string similarity
        best_match = None
        best_score = 0
        for r in results:
            candidate_name = r.get("name", "")
            score = similar(normalized_title, normalize_title(candidate_name))
            if score > best_score:
                best_score = score
                best_match = r

        # If similarity is too low (<0.6), probably wrong result
        if best_match and best_score >= 0.6:
            return transform_igdb(best_match)
        else:
            print(f"No strong IGDB match found for {title}, best score={best_score}")
            return {}

    except requests.HTTPError as e:
        print(f"IGDB fetch failed for {title} — HTTP error: {e.response.text}")
    except Exception as e:
        print(f"IGDB fetch failed for {title}: {e}")
    return {}




def transform_igdb(raw_game):
    def format_cover(url):
        return "https:" + url.replace("t_thumb", "t_cover_big")

    def format_screenshot(url):
        return "https:" + url.replace("t_thumb", "t_screenshot_med")

    transformed = {
        "id": raw_game.get("id"),
        "name": raw_game.get("name"),
        "summary": raw_game.get("summary"),
        "storyline": raw_game.get("storyline"),
        "total_rating": raw_game.get("total_rating"),
        "first_release_date": raw_game.get("first_release_date"),
    }

    if "cover" in raw_game and raw_game["cover"].get("url"):
        transformed["cover_url"] = format_cover(raw_game["cover"]["url"])

    if "screenshots" in raw_game:
        transformed["screenshots"] = [format_screenshot(s["url"]) for s in raw_game["screenshots"] if s.get("url")]

    if "websites" in raw_game:
        transformed["websites"] = [w["url"] for w in raw_game["websites"] if w.get("url")]

    for field in ["player_perspectives", "game_engines", "game_modes", "genres"]:
        if field in raw_game:
            transformed[field] = [item["name"] for item in raw_game[field] if item.get("name")]

    return transformed


def parse_expiry(expiry_str):
    """Convert expiry string to UTC datetime or None."""
    if not expiry_str or expiry_str == "N/A":
        return None
    try:
        # Format: "2025-08-21 23:59:00"
        dt = datetime.strptime(expiry_str, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        print(f"[WARN] Could not parse expiry date: {expiry_str}")
        return None


def send_fcm_notification(game, urgent=False):
    if urgent:
        title = "⚡ LAST CHANCE: Free Game Ending Soon!"
        body = f"{game['name']} is FREE on {game['store']} but HURRY—offer ends within 24h!"
    else:
        title = "FREE GAME ALERT 🎮"
        body = f"{game['name']} is now FREE on {game['store']}!"

    message = messaging.Message(
        topic="free_games",
        notification=messaging.Notification(
            title=title,
            body=body
        ),
        data={
            "game_name": game["name"],
            "worth": game["worth"],
            "store": game["store"],
            "expiry_date": game["expiry_date"],
            "click_action": "OPEN_GAME_PAGE"
        }
    )
    try:
        messaging.send(message)
        print(f"Notification sent for {game['name']} (urgent={urgent})")
    except Exception as e:
        print(f"Notification failed for {game['name']}: {e}")


def main():
    print("Fetching GamerPower freebies...")
    gp_games = fetch_gamerpower_games()
    old_list = read_local_json()

    old_dict = {g["gamerpower_id"]: g for g in old_list}
    enriched_games = []
    now = datetime.now(timezone.utc)

    updated = False

    for gp_game in gp_games:
        igdb_data = fetch_igdb_data(gp_game["title"])
        merged_game = {**gp_game, **igdb_data}

        old_game = old_dict.get(gp_game["gamerpower_id"])
        expiry_dt = parse_expiry(gp_game.get("expiry_date"))
        urgent_needed = False

        # --- Debugging log ---
        if expiry_dt:
            hours_left = (expiry_dt - now).total_seconds() / 3600
            print(f"[CHECK] {gp_game['title']} expires in {hours_left:.2f}h")
        else:
            print(f"[CHECK] {gp_game['title']} has no expiry")

        # --- Expiry Check ---
        if expiry_dt and (expiry_dt - now).total_seconds() <= 86400:
            already_notified = old_game.get("urgent_notified", False) if old_game else False
            if not already_notified:
                urgent_needed = True
                merged_game["urgent_notified"] = True
                updated = True
            else:
                merged_game["urgent_notified"] = True
        else:
            if old_game and old_game.get("urgent_notified", False):
                merged_game["urgent_notified"] = True

        # --- Notifications ---
        if not old_game:  # new game
            send_fcm_notification(merged_game, urgent=False)
            updated = True

        if urgent_needed:
            send_fcm_notification(merged_game, urgent=True)

        enriched_games.append(merged_game)

    if updated or gp_games != old_list:
        firestore_client.collection("freebies").document("games").set({
            "games": enriched_games
        })
        write_local_json(enriched_games)
        print("Database & local JSON updated.")
    else:
        print("No new freebies or urgent expiries.")
