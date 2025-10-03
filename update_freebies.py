import os
import json
import re
import string
import unicodedata
import requests
from datetime import datetime
from google.cloud import firestore
from google.oauth2 import service_account
import firebase_admin
from firebase_admin import messaging

# -----------------
# ENV VARIABLES
# -----------------
FCM_TOPIC = "/topics/free_games"
GAMERPOWER_API = "https://www.gamerpower.com/api/filter?platform=epic-games-store.steam.gog.origin&type=game&sort-by=date"
LOCAL_JSON_FILE = "freebies.json"
SKIPPED_JSON_FILE = "skipped_games.json"  # quarantine log

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
_ROMAN_MAP = {
    "i":"1","ii":"2","iii":"3","iv":"4","v":"5","vi":"6","vii":"7","viii":"8","ix":"9","x":"10",
    "xi":"11","xii":"12","xiii":"13","xiv":"14","xv":"15","xvi":"16","xvii":"17","xviii":"18","xix":"19","xx":"20"
}

_EDITION_KEYWORDS = {
    "remastered", "definitive", "goty", "complete", "hd",
    "ultimate", "anniversary", "collection", "trilogy", "bundle",
    "director", "redux", "reloaded", "remake"
}


def normalize_title(title: str) -> str:
    """Normalize game titles for strict equality checks."""
    if not title:
        return ""
    t = unicodedata.normalize("NFKD", title)
    t = "".join(ch for ch in t if not unicodedata.combining(ch))
    t = t.lower()
    t = t.replace("&", " and ")
    t = re.sub(r"[™®©]", "", t)
    t = re.sub(rf"[{re.escape(string.punctuation)}]", " ", t)
    tokens = t.split()
    tokens = [_ROMAN_MAP.get(tok, tok) for tok in tokens]
    t = " ".join(tokens)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def is_confusing_match(gp_title: str, igdb_name: str) -> bool:
    """Reject sequels/editions that GamerPower title didn’t specify."""
    gp_norm = normalize_title(gp_title)
    igdb_norm = normalize_title(igdb_name)

    gp_digits = re.findall(r"\d+", gp_norm)
    igdb_digits = re.findall(r"\d+", igdb_norm)
    if igdb_digits and not gp_digits:
        return True

    for kw in _EDITION_KEYWORDS:
        if kw in igdb_norm and kw not in gp_norm:
            return True

    return False


def detect_store(offer):
    """Detect store from platforms/description/title."""
    title = offer.get("title", "").lower()
    desc = (offer.get("description", "") or "").lower()
    platforms = (offer.get("platforms", "") or "").lower()

    if "steam" in title or "steam" in platforms:
        return "Steam"
    if "epic" in title or "epic" in platforms:
        return "Epic Games Store"
    if "gog" in title or "gog" in platforms:
        return "GoG"
    if "origin" in title or "origin" in platforms:
        return "Origin"
    if "indiegala" in desc or "indiegala" in platforms:
        return "IndieGala"
    if "stove" in desc or "stove" in platforms:
        return "STOVE"
    if "itch" in desc or "itch" in platforms:
        return "Itch.io"
    if "drm-free" in platforms:
        return "DRM-Free"
    return "Unknown"

def merge_game_data(gp_game, igdb_data):
    """Merge IGDB + GamerPower data into final object."""
    merged = {**gp_game, **igdb_data}
    # overwrite websites with open_giveaway_url
    merged["open_giveaway_url"] = gp_game.get("open_giveaway_url")
    return merged

def fetch_gamerpower_games():
    try:
        resp = requests.get(GAMERPOWER_API, timeout=10)
        resp.raise_for_status()
        offers = resp.json()
        games = []
        for offer in offers:
            # skip non-full games (keys, DLC, etc.)
            if "Key Giveaway" in offer["title"]:
                continue

            clean_title = re.sub(r"\s*\(.*?\)", "", offer["title"])
            clean_title = re.sub(r"\s*Giveaway", "", clean_title).strip()

            store = detect_store(offer)
            worth = offer.get("worth", "$0.00").replace("$", "").strip() or "0.00"

            games.append({
                "gamerpower_id": offer["id"],
                "title": clean_title,
                "worth": worth,
                "store": store,
                "expiry_date": offer.get("end_date", "N/A"),
                "open_giveaway_url": offer.get("open_giveaway_url") or offer.get("open_giveaway")
            })
        return games
    except Exception as e:
        print(f"Error fetching GamerPower data: {e}")
        return []


def read_local_json(file_path="freebies.json"):
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


def write_local_json(data, file_path=LOCAL_JSON_FILE):
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def append_skipped(game, reason):
    """Save skipped game info for later manual review."""
    entry = {**game, "reason": reason, "skipped_at": datetime.utcnow().isoformat()}
    skipped = []
    if os.path.exists(SKIPPED_JSON_FILE):
        with open(SKIPPED_JSON_FILE, "r", encoding="utf-8") as f:
            try:
                skipped = json.load(f)
            except:
                skipped = []
    skipped.append(entry)
    with open(SKIPPED_JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(skipped, f, indent=2, ensure_ascii=False)


def fetch_igdb_data(title: str, normalized_target: str, gp_game: dict):
    url = "https://api.igdb.com/v4/games"
    headers = {
        "Client-ID": IGDB_CLIENT_ID,
        "Authorization": f"Bearer {IGDB_ACCESS_TOKEN}",
    }
    body = f'''
    search "{title}";
    fields id, name, cover.url, total_rating, storyline, first_release_date,
           summary, genres.name, player_perspectives.name, game_engines.name,
           game_modes.name, screenshots.url, websites.url, platforms;
    limit 25;
    '''
    try:
        resp = requests.post(url, headers=headers, data=body.strip(), timeout=10)
        resp.raise_for_status()
        results = resp.json() or []
        for r in results:
            candidate_name = r.get("name", "") or ""
            if normalize_title(candidate_name) == normalized_target:
                if is_confusing_match(title, candidate_name):
                    append_skipped(gp_game, f"Confusing match with '{candidate_name}'")
                    continue
                platforms = [str(p) for p in r.get("platforms", [])]
                if not any(pid in platforms for pid in ("6", "14", "92")):
                    append_skipped(gp_game, f"Non-PC platform match: {candidate_name}")
                    continue
                return transform_igdb(r)
        append_skipped(gp_game, "No strict safe IGDB match")
        return {}
    except Exception as e:
        append_skipped(gp_game, f"IGDB fetch error: {e}")
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
    gp_games = fetch_gamerpower_games()
    old_list = read_local_json()
    if gp_games != old_list:
        print("Freebies updated, fetching IGDB details with STRICT SAFE matching...")
        enriched_games = []
        for gp_game in gp_games:
            gp_norm = normalize_title(gp_game["title"])
            igdb_data = fetch_igdb_data(gp_game["title"], gp_norm, gp_game)

            if igdb_data:
                merged_game = merge_game_data(gp_game, igdb_data)
                enriched_games.append(merged_game)

        old_ids = {g["gamerpower_id"] for g in old_list}
        #for game in enriched_games:
            #if game["gamerpower_id"] not in old_ids:
                #send_fcm_notification(game)

        firestore_client.collection("all_freebies").document("games").set({"games": enriched_games})
        write_local_json(gp_games)
        print(f"Saved {len(enriched_games)} strict-match games to Firestore.")
    else:
        print("No changes in freebies.")



if __name__ == "__main__":
    main()
