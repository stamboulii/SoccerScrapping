#!/usr/bin/env python3
"""
Advanced Asynchronous Soccer Data Scraper
Scrapes multiple websites simultaneously for maximum performance
"""

import asyncio
import aiohttp
from bs4 import BeautifulSoup
import time
import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import logging
from pathlib import Path
import random

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ScrapingResult:
    """Data class to store scraping results"""
    url: str
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    response_time: float = 0.0
    status_code: Optional[int] = None

class AsyncSoccerScraper:
    """
    Advanced asynchronous scraper for soccer data
    
    Features:
    - Scrapes multiple sites simultaneously
    - Automatic retry on failures
    - Rate limiting to avoid getting blocked
    - Rotating user agents
    - Comprehensive error handling
    """
    
    def __init__(self, max_concurrent: int = 10, delay_between_requests: float = 0.1):
        """
        Initialize the async scraper
        
        Args:
            max_concurrent: Maximum number of simultaneous requests
            delay_between_requests: Delay between requests to avoid rate limiting
        """
        self.max_concurrent = max_concurrent
        self.delay_between_requests = delay_between_requests
        self.session = None
        self.results = []
        
        # User agents to rotate (avoid detection)
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0'
        ]
        
        # Soccer websites to scrape
        self.soccer_sites = {
            'bbc_sport': {
                'url': 'https://www.bbc.com/sport/football',
                'parser': self._parse_bbc_sport
            },
            'sky_sports': {
                'url': 'https://www.skysports.com/football',
                'parser': self._parse_sky_sports
            },
            'espn': {
                'url': 'https://www.espn.com/soccer/',
                'parser': self._parse_espn
            },
            'goal': {
                'url': 'https://www.goal.com/en',
                'parser': self._parse_goal
            },
            'transfermarkt': {
                'url': 'https://www.transfermarkt.com',
                'parser': self._parse_transfermarkt
            }
        }
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            connector=aiohttp.TCPConnector(limit=self.max_concurrent)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    def _get_random_user_agent(self) -> str:
        """Get a random user agent to avoid detection"""
        return random.choice(self.user_agents)
    
    async def _fetch_url(self, url: str, max_retries: int = 3) -> ScrapingResult:
        """
        Fetch a single URL with retry logic
        
        Args:
            url: URL to fetch
            max_retries: Maximum number of retry attempts
            
        Returns:
            ScrapingResult object with the results
        """
        start_time = time.time()
        
        for attempt in range(max_retries):
            try:
                headers = {
                    'User-Agent': self._get_random_user_agent(),
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1'
                }
                
                async with self.session.get(url, headers=headers) as response:
                    content = await response.text()
                    response_time = time.time() - start_time
                    
                    if response.status == 200:
                        logger.info(f"✅ Successfully fetched {url} (attempt {attempt + 1})")
                        return ScrapingResult(
                            url=url,
                            success=True,
                            data={'html': content, 'headers': dict(response.headers)},
                            response_time=response_time,
                            status_code=response.status
                        )
                    else:
                        logger.warning(f"⚠️ HTTP {response.status} for {url} (attempt {attempt + 1})")
                        
            except asyncio.TimeoutError:
                logger.warning(f"⏱️ Timeout for {url} (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    
            except Exception as e:
                logger.error(f"❌ Error fetching {url} (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
        
        # All retries failed
        response_time = time.time() - start_time
        return ScrapingResult(
            url=url,
            success=False,
            error=f"Failed after {max_retries} attempts",
            response_time=response_time
        )
    
    async def scrape_multiple_urls(self, urls: List[str]) -> List[ScrapingResult]:
        """
        Scrape multiple URLs simultaneously
        
        Args:
            urls: List of URLs to scrape
            
        Returns:
            List of ScrapingResult objects
        """
        logger.info(f"🚀 Starting to scrape {len(urls)} URLs simultaneously")
        
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def scrape_with_limit(url):
            async with semaphore:
                # Add delay to avoid overwhelming servers
                await asyncio.sleep(self.delay_between_requests)
                return await self._fetch_url(url)
        
        # Create tasks for all URLs
        tasks = [scrape_with_limit(url) for url in urls]
        
        # Execute all tasks and gather results
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and process results
        valid_results = []
        for result in results:
            if isinstance(result, ScrapingResult):
                valid_results.append(result)
            else:
                logger.error(f"❌ Task failed with exception: {result}")
        
        logger.info(f"✅ Completed scraping {len(valid_results)} URLs")
        return valid_results
    
    def _parse_bbc_sport(self, html: str) -> Dict[str, Any]:
        """Parse BBC Sport football page"""
        soup = BeautifulSoup(html, 'html.parser')
        
        data = {
            'site': 'BBC Sport',
            'title': soup.find('title').get_text() if soup.find('title') else 'No title',
            'articles': [],
            'matches': [],
            'headlines': []
        }
        
        # Find articles
        articles = soup.find_all('h3', class_='gs-c-promo-heading__title', limit=10)
        for article in articles:
            if article.get_text().strip():
                data['articles'].append(article.get_text().strip())
        
        # Find headlines
        headlines = soup.find_all('h2', limit=5)
        for headline in headlines:
            if headline.get_text().strip():
                data['headlines'].append(headline.get_text().strip())
        
        return data
    
    def _parse_sky_sports(self, html: str) -> Dict[str, Any]:
        """Parse Sky Sports football page"""
        soup = BeautifulSoup(html, 'html.parser')
        
        data = {
            'site': 'Sky Sports',
            'title': soup.find('title').get_text() if soup.find('title') else 'No title',
            'articles': [],
            'matches': [],
            'news': []
        }
        
        # Find news items
        news_items = soup.find_all('h3', limit=10)
        for item in news_items:
            if item.get_text().strip():
                data['news'].append(item.get_text().strip())
        
        return data
    
    def _parse_espn(self, html: str) -> Dict[str, Any]:
        """Parse ESPN soccer page"""
        soup = BeautifulSoup(html, 'html.parser')
        
        data = {
            'site': 'ESPN',
            'title': soup.find('title').get_text() if soup.find('title') else 'No title',
            'articles': [],
            'scores': [],
            'headlines': []
        }
        
        # Find headlines
        headlines = soup.find_all('h1', limit=5)
        for headline in headlines:
            if headline.get_text().strip():
                data['headlines'].append(headline.get_text().strip())
        
        return data
    
    def _parse_goal(self, html: str) -> Dict[str, Any]:
        """Parse Goal.com page"""
        soup = BeautifulSoup(html, 'html.parser')
        
        data = {
            'site': 'Goal.com',
            'title': soup.find('title').get_text() if soup.find('title') else 'No title',
            'articles': [],
            'transfer_news': [],
            'match_reports': []
        }
        
        # Find articles
        articles = soup.find_all('h3', limit=10)
        for article in articles:
            if article.get_text().strip():
                data['articles'].append(article.get_text().strip())
        
        return data
    
    def _parse_transfermarkt(self, html: str) -> Dict[str, Any]:
        """Parse Transfermarkt page"""
        soup = BeautifulSoup(html, 'html.parser')
        
        data = {
            'site': 'Transfermarkt',
            'title': soup.find('title').get_text() if soup.find('title') else 'No title',
            'transfers': [],
            'player_values': [],
            'market_updates': []
        }
        
        # Find player-related content
        player_links = soup.find_all('a', limit=10)
        for link in player_links:
            if link.get_text().strip() and 'player' in link.get('href', '').lower():
                data['transfers'].append(link.get_text().strip())
        
        return data
    
    async def scrape_all_soccer_sites(self) -> Dict[str, Any]:
        """
        Scrape all configured soccer websites
        
        Returns:
            Dictionary with parsed data from all sites
        """
        logger.info("🌍 Starting comprehensive soccer data scraping")
        
        urls = [site_info['url'] for site_info in self.soccer_sites.values()]
        raw_results = await self.scrape_multiple_urls(urls)
        
        parsed_data = {}
        
        for result in raw_results:
            if result.success:
                # Find which site this URL belongs to
                site_name = None
                parser = None
                
                for name, site_info in self.soccer_sites.items():
                    if site_info['url'] == result.url:
                        site_name = name
                        parser = site_info['parser']
                        break
                
                if site_name and parser:
                    try:
                        parsed_data[site_name] = parser(result.data['html'])
                        parsed_data[site_name]['scraping_info'] = {
                            'response_time': result.response_time,
                            'status_code': result.status_code,
                            'timestamp': time.time()
                        }
                        logger.info(f"✅ Successfully parsed {site_name}")
                    except Exception as e:
                        logger.error(f"❌ Error parsing {site_name}: {str(e)}")
                        parsed_data[site_name] = {
                            'error': str(e),
                            'scraping_info': {
                                'response_time': result.response_time,
                                'status_code': result.status_code,
                                'timestamp': time.time()
                            }
                        }
            else:
                logger.error(f"❌ Failed to scrape {result.url}: {result.error}")
        
        return parsed_data
    
    def save_results_to_json(self, data: Dict[str, Any], filename: str = None):
        """
        Save scraping results to JSON file
        
        Args:
            data: Data to save
            filename: Optional filename, defaults to timestamped file
        """
        if filename is None:
            timestamp = int(time.time())
            filename = f"soccer_scraping_results_{timestamp}.json"
        
        # Create data directory if it doesn't exist
        data_dir = Path("data/scraping_results")
        data_dir.mkdir(parents=True, exist_ok=True)
        
        filepath = data_dir / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"💾 Results saved to {filepath}")
        except Exception as e:
            logger.error(f"❌ Error saving results: {str(e)}")
    
    async def continuous_scraping(self, interval_minutes: int = 30):
        """
        Continuously scrape soccer sites at specified intervals
        
        Args:
            interval_minutes: Minutes between scraping cycles
        """
        logger.info(f"🔄 Starting continuous scraping (every {interval_minutes} minutes)")
        
        while True:
            try:
                # Scrape all sites
                data = await self.scrape_all_soccer_sites()
                
                # Save results
                self.save_results_to_json(data)
                
                # Print summary
                successful_sites = len([site for site in data.values() if 'error' not in site])
                logger.info(f"📊 Scraping cycle completed: {successful_sites}/{len(self.soccer_sites)} sites successful")
                
                # Wait for next cycle
                await asyncio.sleep(interval_minutes * 60)
                
            except KeyboardInterrupt:
                logger.info("🛑 Continuous scraping stopped by user")
                break
            except Exception as e:
                logger.error(f"❌ Error in continuous scraping: {str(e)}")
                await asyncio.sleep(60)  # Wait 1 minute before retrying

# Example usage and testing functions
async def test_basic_async_scraping():
    """Test basic asynchronous scraping functionality"""
    print("🧪 Testing basic async scraping...")
    
    test_urls = [
        'https://www.bbc.com/sport/football',
        'https://www.skysports.com/football',
        'https://www.espn.com/soccer/'
    ]
    
    async with AsyncSoccerScraper(max_concurrent=3) as scraper:
        start_time = time.time()
        results = await scraper.scrape_multiple_urls(test_urls)
        end_time = time.time()
        
        print(f"⏱️ Scraped {len(results)} URLs in {end_time - start_time:.2f} seconds")
        
        for result in results:
            if result.success:
                print(f"✅ {result.url} - {result.response_time:.2f}s")
            else:
                print(f"❌ {result.url} - {result.error}")

async def test_comprehensive_scraping():
    """Test comprehensive soccer site scraping"""
    print("🧪 Testing comprehensive soccer scraping...")
    
    async with AsyncSoccerScraper(max_concurrent=5) as scraper:
        start_time = time.time()
        data = await scraper.scrape_all_soccer_sites()
        end_time = time.time()
        
        print(f"⏱️ Comprehensive scraping completed in {end_time - start_time:.2f} seconds")
        
        for site_name, site_data in data.items():
            if 'error' not in site_data:
                print(f"✅ {site_name}: {site_data['title']}")
                if 'articles' in site_data:
                    print(f"   📰 Found {len(site_data['articles'])} articles")
            else:
                print(f"❌ {site_name}: {site_data['error']}")
        
        # Save results
        scraper.save_results_to_json(data, "test_comprehensive_results.json")

async def main():
    """Main function to demonstrate async scraping capabilities"""
    print("🚀 Welcome to Advanced Async Soccer Scraper!")
    print("=" * 50)
    
    # Test basic functionality
    await test_basic_async_scraping()
    print("\n" + "=" * 50)
    
    # Test comprehensive scraping
    await test_comprehensive_scraping()
    print("\n" + "=" * 50)
    
    print("✅ All tests completed!")
    print("💡 Check the 'data/scraping_results' folder for saved results")

if __name__ == "__main__":
    # Run the async scraper
    asyncio.run(main())