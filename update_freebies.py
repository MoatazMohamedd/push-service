import os
import json
import re
import string
import unicodedata
import requests
import time
from datetime import datetime, timezone, timedelta
from google.cloud import firestore
from google.oauth2 import service_account
import firebase_admin
from firebase_admin import messaging

# -----------------
# ENV VARIABLES
# -----------------
GAMERPOWER_API = "https://www.gamerpower.com/api/filter?type=game"
SKIPPED_JSON_FILE = "skipped_games.json"

IGDB_CLIENT_ID = os.getenv("IGDB_CLIENT_ID")
IGDB_ACCESS_TOKEN = os.getenv("IGDB_ACCESS_TOKEN")

FIREBASE_CREDENTIALS_JSON = os.getenv("FIREBASE_CREDENTIALS_JSON")
FIRESTORE_PROJECT_ID = os.getenv("FIRESTORE_PROJECT_ID")

# -----------------
# MANUAL GAMES (Added to top of Firestore array)
# -----------------
MANUAL_GAMES = [
   
]


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
    """Reject sequels/editions that GamerPower title didn't specify."""
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
    merged["open_giveaway_url"] = gp_game.get("open_giveaway_url")
    return merged

def append_skipped(game, reason):
    """Log skipped games to file."""
    entry = {**game, "reason": reason, "skipped_at": datetime.now(timezone.utc).isoformat()}
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

def fetch_gamerpower_games():
    """Fetch current free games from GamerPower API."""
    try:
        resp = requests.get(GAMERPOWER_API, timeout=10)
        resp.raise_for_status()
        offers = resp.json()
        games = []

        for offer in offers:
            if "Key Giveaway" in offer["title"]:
                continue

            # Set expiry date - 30 days from now if not provided
            end_date = offer.get("end_date")
            if not end_date or end_date == "N/A":
                expiry_date = (datetime.utcnow() + timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
            else:
                expiry_date = end_date

            # Clean title
            clean_title = re.sub(r"\s*\(.*?\)", "", offer["title"])
            clean_title = re.sub(r"\s*Giveaway", "", clean_title).strip()

            store = detect_store(offer)
            worth = offer.get("worth", "$0.00").replace("$", "").strip() or "0.00"

            games.append({
                "gamerpower_id": offer["id"],
                "title": clean_title,
                "worth": worth,
                "store": store,
                "expiry_date": expiry_date,
                "open_giveaway_url": offer.get("open_giveaway_url") or offer.get("open_giveaway")
            })
        return games
    except Exception as e:
        print(f"❌ Error fetching GamerPower data: {e}")
        return []

def fetch_igdb_data(title: str, normalized_target: str, gp_game: dict):
    """Fetch game data from IGDB API with rate limiting."""
    time.sleep(0.25)  # Rate limit: 4 requests/second max
    
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
    """Transform IGDB raw data to our format."""
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

def is_expiring_today(expiry_date: str) -> bool:
    """Check if expiry_date matches today's date (UTC)."""
    try:
        exp_date = datetime.fromisoformat(expiry_date.replace("Z", "+00:00"))
    except ValueError:
        return False
    today = datetime.now(timezone.utc).date()
    return exp_date.date() == today

def send_fcm_notification(game):
    """Send FCM push notification for new free game."""
    message = messaging.Message(
        topic="free_games",
        notification=messaging.Notification(
            title=f"{game['name']} Just Turned FREE!",
            body=f"Claim it on {game['store']} fast before it's gone!"
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
        print(f"✅ Notification sent for {game['name']}")
    except Exception as e:
        print(f"❌ Notification failed for {game['name']}: {e}")

def send_expiry_reminders(games, firestore_games):
    """Send reminder notifications for games expiring today."""
    firestore_map = {g["gamerpower_id"]: g for g in firestore_games}
    
    for game in games:
        if is_expiring_today(game["expiry_date"]):
            old_entry = firestore_map.get(game["gamerpower_id"], {})
            already_sent = old_entry.get("reminder_sent", False)
            
            if not already_sent:
                message = messaging.Message(
                    topic="free_games",
                    notification=messaging.Notification(
                        title=f"Last Chance for {game['name']}!",
                        body=f"Free offer ends TODAY on {game['store']}. Claim it now before it's gone forever!"
                    ),
                    data={
                        "game_name": game["name"],
                        "store": game["store"],
                        "expiry_date": game["expiry_date"],
                        "click_action": "OPEN_GAME_PAGE"
                    }
                )
                try:
                    messaging.send(message)
                    print(f"⏰ Expiry reminder sent for {game['name']}")
                    game["reminder_sent"] = True
                except Exception as e:
                    print(f"❌ Expiry reminder failed for {game['name']}: {e}")

def get_firestore_games():
    """Fetch current games from Firestore."""
    try:
        firestore_doc = firestore_client.collection("all_freebies").document("games").get()
        if firestore_doc.exists:
            firestore_data = firestore_doc.to_dict() or {}
            return firestore_data.get("games", [])
        return []
    except Exception as e:
        print(f"❌ Firestore read failed: {e}")
        return []

def update_firestore_games(games):
    """Update games in Firestore."""
    try:
        firestore_client.collection("all_freebies").document("games").set({"games": games})
        print(f"✅ Saved {len(games)} games to Firestore")
    except Exception as e:
        print(f"❌ Firestore write failed: {e}")

def main():
    print("🎮 Fetching GamerPower freebies...")
    gp_games = fetch_gamerpower_games()
    
    if not gp_games:
        print("⚠️  No games fetched from GamerPower. Exiting.")
        return
    
    print(f"📥 Found {len(gp_games)} games from GamerPower")
    
    # Get current Firestore games
    print("🔍 Fetching games from Firestore...")
    firestore_games = get_firestore_games()
    
    # Compare IDs
    firestore_ids = {g["gamerpower_id"] for g in firestore_games}
    gp_ids = {g["gamerpower_id"] for g in gp_games}
    
    added_ids = gp_ids - firestore_ids
    removed_ids = firestore_ids - gp_ids
    
    if not added_ids and not removed_ids:
        print("✨ No changes detected. Everything is up to date!")
        return
    
    print(f"\n📊 Changes detected:")
    if added_ids:
        print(f"  ➕ Added: {len(added_ids)} games")
    if removed_ids:
        print(f"  ➖ Removed: {len(removed_ids)} games")
    
    # Create map of existing Firestore games for preserving manual edits
    firestore_map = {g["gamerpower_id"]: g for g in firestore_games}
    
    # Enrich games with IGDB data
    enriched_games = []
    for gp_game in gp_games:
        gp_id = gp_game["gamerpower_id"]
        
        # Check if this is a new game
        is_new_game = gp_id in added_ids
        
        # Fetch IGDB data for new games OR if existing game lacks IGDB data
        should_enrich = is_new_game
        if gp_id in firestore_map:
            existing = firestore_map[gp_id]
            # Check if IGDB data is missing
            if not existing.get("id") or not existing.get("name"):
                should_enrich = True
        
        if should_enrich:
            print(f"🔎 Enriching game: {gp_game['title']}")
            gp_norm = normalize_title(gp_game["title"])
            igdb_data = fetch_igdb_data(gp_game["title"], gp_norm, gp_game)
            
            if igdb_data:
                merged_game = merge_game_data(gp_game, igdb_data)
                
                # If game existed, preserve manual edits
                if gp_id in firestore_map:
                    existing = firestore_map[gp_id]
                    final_game = {}
                    for key, value in merged_game.items():
                        # Always refresh API fields
                        if key in ["expiry_date", "worth", "store", "open_giveaway_url"]:
                            final_game[key] = value
                        # Preserve manual edits for other fields
                        elif key in existing and existing[key] not in [None, "", [], {}]:
                            final_game[key] = existing[key]
                        else:
                            final_game[key] = value
                    enriched_games.append(final_game)
                else:
                    # Completely new game
                    enriched_games.append(merged_game)
                
                # Send notification only for new games
                if is_new_game:
                    send_fcm_notification(merged_game)
            else:
                print(f"⚠️  Skipped {gp_game['title']} (no IGDB match)")
        else:
            # Game exists and has IGDB data - just update API fields
            existing = firestore_map[gp_id]
            merged = {}
            for key in existing.keys():
                # Always refresh API fields
                if key in gp_game and key in ["expiry_date", "worth", "store", "open_giveaway_url"]:
                    merged[key] = gp_game[key]
                else:
                    merged[key] = existing[key]
            enriched_games.append(merged)
    
    # Send expiry reminders before updating Firestore
    print("\n⏰ Checking for expiring games...")
    send_expiry_reminders(enriched_games, firestore_games)
    
    # Prepend manual games to the top of the array
    if MANUAL_GAMES:
        print(f"\n📌 Adding {len(MANUAL_GAMES)} manual games to the top of the list")
        final_games = MANUAL_GAMES + enriched_games
    else:
        final_games = enriched_games
    
    # Update Firestore with new list
    update_firestore_games(final_games)
    
    if removed_ids:
        print(f"🗑️  Removed {len(removed_ids)} expired games")
    
    print(f"\n✅ Done! {len(final_games)} games now in Firestore ({len(MANUAL_GAMES)} manual + {len(enriched_games)} from API)")

if __name__ == "__main__":
    main()