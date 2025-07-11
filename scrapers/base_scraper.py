import aiohttp
import asyncio
import json
from typing import List, Dict
from bs4 import BeautifulSoup


class AsyncSoccerScraper:
    def __init__(self, max_concurrent: int = 5):
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None
        self.results: Dict[str, Dict] = {}

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.session.close()

    async def fetch(self, url: str) -> Dict:
        try:
            async with self.semaphore:
                async with self.session.get(url, timeout=10) as response:
                    content = await response.text()
                    return {
                        "status": response.status,
                        "content": content,
                        "url": str(response.url)
                    }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }

    def parse(self, html: str) -> Dict:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            title = soup.title.string if soup.title else "No title"
            articles = []

            for h3 in soup.find_all('h3'):
                articles.append({
                    "name": h3.get_text(strip=True),
                    "age": 25,
                    "club": "Unknown"
                })

            return {
                "title": title,
                "articles": articles
            }
        except Exception as e:
            return {
                "error": str(e)
            }

    async def scrape_url(self, url: str) -> None:
        result = await self.fetch(url)
        if result.get("status") == 200:
            parsed = self.parse(result["content"])
            self.results[url] = parsed
        else:
            self.results[url] = {"error": result.get("error", "Failed to fetch")}

    async def scrape_all_soccer_sites(self) -> Dict[str, Dict]:
        urls = [
            "https://www.bbc.com/sport/football",
            "https://www.skysports.com/football",
        ]
        tasks = [self.scrape_url(url) for url in urls]
        await asyncio.gather(*tasks)
        return self.results

    async def scrape_multiple_urls(self, urls: List[str]) -> None:
        tasks = [self.scrape_url(url) for url in urls]
        await asyncio.gather(*tasks)

    def save_results_to_json(self, data: Dict, filename: str = "results.json") -> None:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
