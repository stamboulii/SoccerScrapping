#!/usr/bin/env python3
"""
Simplified Soccer Data Scraper Main Application
"""

import sys
from pathlib import Path
import pandas as pd
import asyncio
from config import LOG_FILE_NAME, LOG_LEVEL, LOG_TO_CONSOLE, LOG_TO_FILE

sys.path.append(str(Path(__file__).parent))

try:
    from loguru import logger
    from database.sqlite_connection import db_manager, init_database, generate_sample_data
    LOGURU_AVAILABLE = True
except ImportError:
    LOGURU_AVAILABLE = False
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

from scrapers.async_scraper import AsyncSoccerScraper

def setup_logging():
    """Configure logging"""
    if LOGURU_AVAILABLE:
        logger.remove()
        if LOG_TO_CONSOLE:
            logger.add(sys.stdout, level=LOG_LEVEL,
                       format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                              "<level>{level: <8}</level> | "
                              "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                              "<level>{message}</level>")
        if LOG_TO_FILE:
            LOG_FILE_NAME.parent.mkdir(parents=True, exist_ok=True)
            logger.add(str(LOG_FILE_NAME), level=LOG_LEVEL,
                       format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
                       rotation="10 MB", retention="1 month")
    else:
        print("Loguru not available, using basic logging")

def show_banner():
    print("""
    ⚽ ===================================== ⚽
         SOCCER DATA SCRAPER PROJECT
         (Simplified SQLite Version)
    ⚽ ===================================== ⚽
    
    A soccer data collection and database system.
    
    Current features:
    - SQLite database storage
    - Sample data generation
    - Basic data queries
    - Async scraping
    - Interactive mode
    """)

def test_basic_scraping():
    try:
        import requests
        from bs4 import BeautifulSoup

        print("🔄 Testing basic web scraping...")
        url = "https://www.bbc.com/sport/football"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            title = soup.find('title')
            print(f"✅ Title: {title.text if title else 'No title'}")
            articles = soup.find_all('h3', limit=3)
            print("📰 Articles:")
            for i, article in enumerate(articles, 1):
                print(f"  {i}. {article.get_text(strip=True)}")
        else:
            print(f"❌ Failed: HTTP {response.status_code}")
    except Exception as e:
        print(f"❌ Scraping error: {e}")

async def run_async_scraping():
    print("🚀 Starting async scraping...")
    async with AsyncSoccerScraper(max_concurrent=5) as scraper:
        data = await scraper.scrape_all_soccer_sites()
        custom_urls = [
            'https://www.bbc.com/sport/football',
            'https://www.skysports.com/football'
        ]
        await scraper.scrape_multiple_urls(custom_urls)
        scraper.save_results_to_json(data)
        print(f"✅ Scraped {len(data)} websites successfully!")

def run_interactive_mode():
    while True:
        print("\n=== Soccer Scraper Interactive Mode ===")
        print("1. Test database connection")
        print("2. Generate sample data")
        print("3. View database statistics")
        print("4. View sample data")
        print("5. Test basic web scraping")
        print("6. Export data to CSV")
        print("7. Exit")
        print("8. Run async scraping")
        print("9. Bulk insert test data")
        print("11. Validate sample player data before insert")

        choice = input("\nSelect an option (1-11): ").strip()

        if choice == '1':
            try:
                stats = db_manager.get_table_stats()
                print("✅ Connection OK\n📊", stats)
            except Exception as e:
                print(f"❌ DB error: {e}")

        elif choice == '2':
            try:
                generate_sample_data()
                print("✅ Sample data generated")
            except Exception as e:
                print(f"❌ Generation error: {e}")

        elif choice == '3':
            try:
                stats = db_manager.get_table_stats()
                for k, v in stats.items():
                    print(f"  {k}: {v}")
            except Exception as e:
                print(f"❌ Stat error: {e}")

        elif choice == '4':
            try:
                for name in ['countries', 'competitions', 'clubs']:
                    df = db_manager.execute_query(f"SELECT * FROM {name} LIMIT 5")
                    print(f"\n📍 {name.title()}:\n", df.to_string(index=False))
            except Exception as e:
                print(f"❌ View error: {e}")

        elif choice == '5':
            test_basic_scraping()

        elif choice == '6':
            try:
                from config import EXPORT_DIR
                EXPORT_DIR.mkdir(parents=True, exist_ok=True)
                for table in ['countries', 'competitions', 'clubs', 'players', 'matches']:
                    df = db_manager.execute_query(f"SELECT * FROM {table}")
                    if not df.empty:
                        path = EXPORT_DIR / f"{table}.csv"
                        df.to_csv(path, index=False)
                        print(f"✅ Exported {table} ({len(df)}) -> {path}")
                    else:
                        print(f"⚠️ {table} is empty")
            except Exception as e:
                print(f"❌ Export error: {e}")

        elif choice == '7':
            print("👋 Goodbye")
            break

        elif choice == '8':
            try:
                asyncio.run(run_async_scraping())
                print("📊 Async scraping completed")
            except Exception as e:
                print(f"❌ Async error: {e}")

        elif choice == '9':
            try:
                test_data = [(f"Player{i}", 20 + i % 10, "Test Club") for i in range(1000)]
                db_manager.bulk_insert_players(test_data)
                print("✅ Bulk insert test done.")
            except Exception as e:
                print(f"❌ Bulk insert failed: {e}")

        elif choice == '11':
            players_raw = [
                {"name": "Erling Haaland", "age": 23, "club": "Manchester City"},
                {"name": "", "age": -1, "club": None},
            ]
            valid_players = []

            def validate(p):
                if not isinstance(p, dict): raise ValueError("Player must be dict")
                if not p.get("name"): raise ValueError("Missing name")
                if not isinstance(p["age"], int) or p["age"] <= 0: raise ValueError("Invalid age")
                if not p.get("club"): raise ValueError("Missing club")

            for p in players_raw:
                try:
                    validate(p)
                    valid_players.append((p["name"], p["age"], p["club"]))
                except ValueError as e:
                    print(f"⚠️ Skipped: {e}")

            if valid_players:
                db_manager.bulk_insert_players(valid_players)
                print("✅ Valid players inserted")
            else:
                print("⚠️ No valid players to insert")

        else:
            print("❌ Invalid option. Try again.")

def main():
    setup_logging()
    show_banner()
    print("🚀 Starting Soccer Data Scraper")

    try:
        print("🔧 Initializing DB...")
        init_database()
        print("✅ DB ready")

        stats = db_manager.get_table_stats()
        if stats.get('countries', 0) == 0:
            print("📊 No data found. Generating sample data...")
            generate_sample_data()

        print("\n🎉 Setup complete")
        return 0

    except Exception as e:
        print(f"❌ Startup failed: {e}")
        return 1

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Soccer Data Scraper")
    parser.add_argument('--interactive', '-i', action='store_true', help='Run in interactive mode')
    parser.add_argument('--sample-data', action='store_true', help='Generate sample data')
    parser.add_argument('--test-scraping', action='store_true', help='Run scraping test')

    args = parser.parse_args()

    if args.interactive:
        setup_logging()
        show_banner()
        init_database()
        run_interactive_mode()
    elif args.sample_data:
        setup_logging()
        init_database()
        generate_sample_data()
        print("✅ Sample data generated")
    elif args.test_scraping:
        setup_logging()
        test_basic_scraping()
    else:
        sys.exit(main())
