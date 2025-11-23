import requests
import re
import time
import random
import csv
from pathlib import Path
from urllib.parse import urljoin, urlparse
from datetime import datetime

MEDIA_EXTENSIONS = re.compile(r'\.(png|jpg|jpeg|gif|ico|css|js|svg|woff|woff2|ttf|eot|mp4|mp3|mov|avi)$', re.IGNORECASE)

class DailyMedCrawler:
    def __init__(self, output_dir="data",resume=False):
        self.base_url = "https://dailymed.nlm.nih.gov"
        self.output_dir = Path(output_dir)
        self.html_dir = self.output_dir / "html"
        self.html_dir.mkdir(parents=True, exist_ok=True)

        # HTTP request headers
        self.headers = {
            "User-Agent": "STUResearchBot/1.0 (matej.splhak@stuba.sk; information retrieval study project)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive"
        }

        self.timeout = 30
        self.sleep_min = 1.0
        self.sleep_max = 2.5
        self.max_retries = 3

        self.visited = set()
        self.page_count = 0

        self.metadata_path = self.output_dir / "crawl_metadata.tsv"
        mode = "a" if (resume and self.metadata_path.exists()) else "w"
        self.metadata_file = open(self.metadata_path, mode, newline="", encoding="utf-8")
        self.metadata_writer = csv.writer(self.metadata_file, delimiter="\t")
        if mode == "w":
          self.metadata_writer.writerow(
              ["url", "status", "http_code", "retries", "saved_path", "scraped_at"]
          )

    def fetch(self, url):
        if url in self.visited:
            return None

        for attempt in range(1, self.max_retries + 1):
            delay = random.uniform(self.sleep_min, self.sleep_max)
            time.sleep(delay)

            try:
                print(f"[{self.page_count}] Fetching (attempt {attempt}): {url}")
                response = requests.get(url, headers=self.headers, timeout=self.timeout)
                response.raise_for_status()
                self.visited.add(url)

                # Log successful fetch
                self.log_metadata(
                    url=url,
                    status="success",
                    http_code=response.status_code,
                    retries=attempt,
                    saved_path=self.get_html_path(url).as_posix(),
                )
                return response.text

            except requests.RequestException as e:
                print(f"ERROR fetching {url} (attempt {attempt}): {e}")
                if attempt == self.max_retries:
                    # Log permanent failure
                    self.log_metadata(
                        url=url,
                        status="failed",
                        http_code=getattr(e.response, "status_code", None),
                        retries=attempt,
                        saved_path="N/A",
                    )
                    return None
                
    def is_allowed_url(self,url_to_check):
        return not MEDIA_EXTENSIONS.search(url_to_check)
    def save_state(self, to_visit):
        state = {
            'to_visit': list(to_visit),
            'visited': list(self.visited),
            'page_count': self.page_count
        }
        state_path = self.output_dir / "crawler_state.json"
        import json
        with open(state_path, 'w') as f:
            json.dump(state, f, indent=2)
        print(f"state saved to {state_path}")

    def load_state(self):
        state_path = self.output_dir / "crawler_state.json"
        if state_path.exists():
            import json
            with open(state_path, 'r') as f:
                state = json.load(f)
            self.visited = set(state['visited'])
            self.page_count = state['page_count']
            return state['to_visit']
        return None

    def extract_links(self, html, current_url):
        links = re.findall(r'href\s*=\s*["\']([^"\']+)["\']', html, re.IGNORECASE)
        absolute_links = set()
    
        for link in links:
            if link.startswith(('javascript:', 'mailto:', 'tel:', '#', 'data:')):
                continue
            
            try:
                full_url = urljoin(current_url, link)
                parsed = urlparse(full_url)
    
                if parsed.netloc == urlparse(self.base_url).netloc:
                  clean_url = parsed.scheme + "://" + parsed.netloc + parsed.path
                  if parsed.query:
                      clean_url += "?" + parsed.query
                  absolute_links.add(clean_url)
            except ValueError as e:
                print(f"Warnign: skipping malformed URL '{link}' from {current_url}: {e}")
                continue

        return absolute_links
        
    def get_html_path(self, url):
        parsed = urlparse(url)
        path_and_query = parsed.path
        
        if parsed.query:
            path_and_query += "?" + parsed.query
        
        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", path_and_query.strip("/")) or "index"

        if len(safe_name) > 200:
            safe_name = safe_name[:200]
        
        return self.html_dir / f"batch_{self.page_count//2000}"/ f"{safe_name}.html"

    def save_html(self, url, html):
        path = self.get_html_path(url)
        path.parent.mkdir(parents=True, exist_ok=True) 
        path.write_text(html, encoding="utf-8")
        self.page_count += 1
        print(f"saved HTML: {path.name}")
        return path

    def log_metadata(self, url, status, http_code, retries, saved_path):
        self.metadata_writer.writerow(
            [url, status, http_code or "N/A", retries, saved_path, datetime.now().isoformat()]
        )
        self.metadata_file.flush()

    def crawl(self, start_url=None, max_pages=100,resume=False):
        if start_url is None:
            start_url = self.base_url

        if resume:
            to_visit = self.load_state()
            if to_visit:
                print(f"resuming crawl with {len(to_visit)} URLs in queue")
            else:
                to_visit = [start_url]
        else:
            to_visit = [start_url]
        max_pages = 2000000
        while to_visit and len(self.visited) < max_pages:
            current_url = to_visit.pop(0)
            html = self.fetch(current_url)
            if not html:
                continue

            self.save_html(current_url, html)
            new_links = self.extract_links(html, current_url)

            for link in new_links:
                if link not in self.visited and link not in to_visit and self.is_allowed_url(link):
                    to_visit.append(link)
            
            if self.page_count % 100 == 0:
                self.save_state(to_visit)

        # Final save
        self.save_state(to_visit)
        print(f"Crawl complete - fetched {self.page_count} pages.")

    def close(self):
        self.metadata_file.close()


def main():
    resume = False
    crawler = DailyMedCrawler(resume=resume)
    try:
        crawler.crawl(max_pages=20, resume=resume)
    finally:
        crawler.close()


if __name__ == "__main__":
    main()
