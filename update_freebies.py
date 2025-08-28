import os
import json
import re
import string
import requests
from datetime import datetime
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
           where platforms = (6);   
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


def send_fcm_notification(game):
    message = messaging.Message(
        topic="free_games",
        notification=messaging.Notification(
            title="FREE GAME ALERT 🎮",
            body=f"{game['name']} is now FREE on {game['store']}!"
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
        print(f"Notification sent for {game['name']}")
    except Exception as e:
        print(f"Notification failed for {game['name']}: {e}")


def main():
    print("Fetching GamerPower freebies...")
    gp_games = fetch_gamerpower_games()   # Step 1: Get API response
    old_list = read_local_json()          # Step 2: Last saved snapshot

    if gp_games != old_list:
        print("Freebies updated, fetching IGDB details...")

        # Step 3: Enrich with IGDB data
        enriched_games = []
        for gp_game in gp_games:
            igdb_data = fetch_igdb_data(gp_game["title"])  # search by title or ID
            enriched_games.append({
                **gp_game,        # keep original gamerpower data
                **igdb_data       # merge IGDB fields (cover, genres, etc.)
            })

        # Step 4: Send notifications only for new games
        old_ids = {g["gamerpower_id"] for g in old_list}
        for game in enriched_games:
            if game["gamerpower_id"] not in old_ids:
                send_fcm_notification(game)

        # Step 5: Overwrite freebies collection
        firestore_client.collection("freebies").document("games").set({
            "games": enriched_games
        })

        # Step 6: Save enriched list for future comparison
        write_local_json(gp_games)  # or enriched_games if you want to compare enriched data next time

    else:
        print("No changes in freebies.")




if __name__ == "__main__":
    main()
