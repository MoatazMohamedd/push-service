def main():
    print("Fetching GamerPower freebies...")
    gp_games = fetch_gamerpower_games()
    old_list = read_local_json()

    old_ids = {g["gamerpower_id"] for g in old_list}
    new_ids = {g["gamerpower_id"] for g in gp_games}
    added_ids = new_ids - old_ids
    removed_ids = old_ids - new_ids

    # -------------------------------------------------
    # Only continue if something changed (less reads!)
    # -------------------------------------------------
    if added_ids or removed_ids:
        print("Detected changes in free games IDs:")
        if added_ids:
            print(f" Added: {added_ids}")
        if removed_ids:
            print(f" Removed: {removed_ids}")

        print("Fetching IGDB details for updated list...")
        enriched_games = []
        for gp_game in gp_games:
            gp_norm = normalize_title(gp_game["title"])
            igdb_data = fetch_igdb_data(gp_game["title"], gp_norm, gp_game)
            if igdb_data:
                merged_game = merge_game_data(gp_game, igdb_data)
                enriched_games.append(merged_game)

        # -------------------------------------------------
        # ✅ READ FIRESTORE ONLY WHEN CHANGES DETECTED
        # -------------------------------------------------
        firestore_doc = firestore_client.collection("all_freebies").document("games").get()
        firestore_data = firestore_doc.to_dict() or {}
        firestore_games = {g["gamerpower_id"]: g for g in firestore_data.get("games", [])}

        final_games = []
        for game in enriched_games:
            gp_id = game["gamerpower_id"]
            if gp_id in firestore_games:
                existing = firestore_games[gp_id]
                merged = {}
                for key, value in game.items():
                    # Always refresh fields that come from API
                    if key in ["expiry_date", "worth", "store", "open_giveaway_url", "reminder_sent"]:
                        merged[key] = value
                    # Preserve manual edits for others
                    elif key in existing and existing[key] not in [None, "", [], {}]:
                        merged[key] = existing[key]
                    else:
                        merged[key] = value
                final_games.append(merged)
            else:
                final_games.append(game)

        # Send notifications for new games
     #   for game in final_games:
          #  if game["gamerpower_id"] in added_ids:
              #  send_fcm_notification(game)

        # Send expiry reminders
       # send_expiry_reminders(final_games, old_list)

        # Update Firestore and local JSON
        firestore_client.collection("test").document("games").set({"games": final_games})
        write_local_json(final_games)

        print(f"Saved {len(final_games)} strict-match games to Firestore (manual edits preserved).")

    else:
        print("No new or removed games. Skipping Firestore read and enrichment.")
