#!/usr/bin/env python3
"""
Simplified Soccer Data Scraper Main Application
Works with basic packages only (no PostgreSQL, no complex scraping)
"""

import sys
from pathlib import Path
import pandas as pd

# Add the project root to the Python path
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

def setup_logging():
    """Configure logging"""
    if LOGURU_AVAILABLE:
        logger.remove()
        logger.add(
            sys.stdout,
            level="INFO",
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        )
        logger.add(
            "logs/soccer_scraper.log",
            level="INFO",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            rotation="10 MB",
            retention="1 month"
        )
    else:
        print("Loguru not available, using basic logging")

def show_banner():
    """Display application banner"""
    banner = """
    ⚽ ===================================== ⚽
         SOCCER DATA SCRAPER PROJECT
         (Simplified SQLite Version)
    ⚽ ===================================== ⚽
    
    A soccer data collection and database system.
    
    Current features:
    - SQLite database storage
    - Sample data generation
    - Basic data queries
    - Interactive mode
    """
    print(banner)

def test_basic_scraping():
    """Test basic web scraping with requests and BeautifulSoup"""
    try:
        import requests
        from bs4 import BeautifulSoup
        
        print("🔄 Testing basic web scraping...")
        
        # Test with a simple sports website
        url = "https://www.bbc.com/sport/football"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            title = soup.find('title')
            print(f"✅ Successfully scraped: {title.text if title else 'No title found'}")
            
            # Find some football-related content
            articles = soup.find_all('h3', limit=3)
            print("📰 Found articles:")
            for i, article in enumerate(articles, 1):
                print(f"  {i}. {article.get_text()[:100]}...")
                
            return True
        else:
            print(f"❌ Failed to scrape: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Scraping test failed: {e}")
        return False

def run_interactive_mode():
    """Run interactive mode for testing and development"""
    while True:
        print("\n=== Soccer Scraper Interactive Mode ===")
        print("1. Test database connection")
        print("2. Generate sample data")
        print("3. View database statistics")
        print("4. View sample data")
        print("5. Test basic web scraping")
        print("6. Export data to CSV")
        print("7. Exit")
        
        choice = input("\nSelect an option (1-7): ").strip()
        
        if choice == '1':
            try:
                stats = db_manager.get_table_stats()
                print("✅ Database connection successful")
                print(f"📊 Tables: {stats}")
            except Exception as e:
                print(f"❌ Database connection failed: {e}")
                
        elif choice == '2':
            try:
                generate_sample_data()
                print("✅ Sample data generated successfully")
            except Exception as e:
                print(f"❌ Error generating sample data: {e}")
                
        elif choice == '3':
            try:
                stats = db_manager.get_table_stats()
                print("\n📊 Database Statistics:")
                for table, count in stats.items():
                    print(f"  {table}: {count} records")
            except Exception as e:
                print(f"❌ Error getting stats: {e}")
                
        elif choice == '4':
            try:
                print("\n🏆 Countries:")
                countries_df = db_manager.execute_query("SELECT * FROM countries LIMIT 5")
                print(countries_df.to_string(index=False))
                
                print("\n🏟️ Competitions:")
                competitions_df = db_manager.execute_query("SELECT * FROM competitions LIMIT 5")
                print(competitions_df.to_string(index=False))
                
                print("\n⚽ Clubs:")
                clubs_df = db_manager.execute_query("SELECT * FROM clubs LIMIT 5")
                print(clubs_df.to_string(index=False))
                
            except Exception as e:
                print(f"❌ Error viewing data: {e}")
                
        elif choice == '5':
            test_basic_scraping()
            
        elif choice == '6':
            try:
                print("📁 Exporting data to CSV files...")
                
                # Export all tables
                tables = ['countries', 'competitions', 'clubs', 'players', 'matches']
                export_dir = Path("data/exports")
                export_dir.mkdir(parents=True, exist_ok=True)
                
                for table in tables:
                    df = db_manager.execute_query(f"SELECT * FROM {table}")
                    if not df.empty:
                        csv_path = export_dir / f"{table}.csv"
                        df.to_csv(csv_path, index=False)
                        print(f"  ✅ Exported {table}: {len(df)} rows -> {csv_path}")
                    else:
                        print(f"  ⚠️  {table}: No data to export")
                        
                print("✅ Export completed")
                
            except Exception as e:
                print(f"❌ Error exporting data: {e}")
                
        elif choice == '7':
            print("👋 Goodbye!")
            break
            
        else:
            print("❌ Invalid option. Please try again.")

def main():
    """Main application function"""
    setup_logging()
    show_banner()
    
    print("🚀 Starting Soccer Data Scraper Project (Simplified)")
    
    try:
        # Initialize database
        print("🔧 Initializing database...")
        init_database()
        print("✅ Database initialized successfully")
        
        # Check if we have sample data
        stats = db_manager.get_table_stats()
        if stats['countries'] == 0:
            print("📊 No sample data found. Generating sample data...")
            generate_sample_data()
        
        print("\n🎉 Application setup completed successfully!")
        print("\nAvailable features:")
        print("✅ SQLite database with soccer schema")
        print("✅ Sample data (countries, competitions, clubs)")
        print("✅ Data export to CSV")
        print("✅ Basic web scraping capabilities")
        print("✅ Interactive mode for testing")
        
        return 0
        
    except Exception as e:
        print(f"❌ Application startup failed: {e}")
        return 1

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Soccer Data Scraper (Simplified)")
    parser.add_argument('--interactive', '-i', action='store_true', 
                       help='Run in interactive mode')
    parser.add_argument('--sample-data', action='store_true',
                       help='Generate sample data')
    parser.add_argument('--test-scraping', action='store_true',
                       help='Test basic web scraping')
    
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
        print("✅ Sample data generated successfully")
    elif args.test_scraping:
        setup_logging()
        test_basic_scraping()
    else:
        # Run main application
        exit_code = main()
        sys.exit(exit_code)