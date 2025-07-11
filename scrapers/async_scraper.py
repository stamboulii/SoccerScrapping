import time
from .base_scraper import AsyncSoccerScraper  # Assurez-vous que c'est bien le bon chemin
from database.postgres_connection import db_manager  # Utilise bien Postgres maintenant

async def test_comprehensive_scraping():
    """Test comprehensive soccer site scraping and insert into PostgreSQL"""
    print("⚽ Testing comprehensive soccer scraping...")

    async with AsyncSoccerScraper(max_concurrent=5) as scraper:
        start_time = time.time()
        data = await scraper.scrape_all_soccer_sites()
        end_time = time.time()

        print(f"⏱️ Scraping completed in {end_time - start_time:.2f} seconds")

        valid_players = []

        for site_name, site_data in data.items():
            if 'error' in site_data:
                print(f"❌ {site_name}: {site_data['error']}")
                continue

            print(f"✅ {site_name}: {site_data.get('title', 'No Title')}")
            articles = site_data.get('articles', [])

            for article in articles:
                try:
                    name = article.get("name")
                    age = article.get("age")
                    club = article.get("club")

                    if name and isinstance(age, int) and club:
                        valid_players.append((name, age, club))
                except Exception as e:
                    print(f"⚠️ Invalid article format: {e}")

        if valid_players:
            db_manager.bulk_insert_players(valid_players)
            print(f"✅ Inserted {len(valid_players)} players into PostgreSQL")
        else:
            print("⚠️ No valid players to insert.")

        scraper.save_results_to_json(data, "test_comprehensive_results.json")
