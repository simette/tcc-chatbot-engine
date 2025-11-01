# Databricks notebook source
!pip install requests beautifulsoup4 lxml

import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
from datetime import datetime
import json
from typing import Dict, List, Optional, Tuple
import pyspark.sql.functions as F

class INSSRetirementScraper:
    """Scraper for INSS retirement information pages"""
    
    def __init__(self):
        self.base_url = "https://www.gov.br/inss/pt-br/direitos-e-deveres/aposentadorias"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Categories to search for
        self.target_categories = [
            "Aposentadoria Especial",
            "Aposentadoria por idade do trabalhador rural",
            "Aposentadoria por idade da pessoa com deficiência",
            "Aposentadoria por Idade Urbana",
            "Aposentadoria por incapacidade permanente",
            "Aposentadoria por tempo de contribuição da pessoa com deficiência",
            "Aposentadoria por tempo de contribuição do professor",
            "Aposentadoria programada",
            "Aposentadoria programada do professor",
            "Regras de Aposentadorias"
        ]
    
    def extract_dates(self, soup: BeautifulSoup) -> Dict[str, Optional[str]]:
        """Extract publication and update dates from the page"""
        dates = {'published': None, 'updated': None}
        
        # Look for the date information in the page
        # Pattern: "Publicado em DD/MM/YYYY HHhMM Atualizado em DD/MM/YYYY HHhMM"
        date_pattern = r'Publicado em (\d{2}/\d{2}/\d{4})'
        update_pattern = r'Atualizado em (\d{2}/\d{2}/\d{4})'
        
        # Search in common locations for date info
        date_containers = soup.find_all(['span', 'div', 'p'], class_=re.compile('date|time|metadata|documentPublished|documentModified'))
        
        page_text = soup.get_text()
        
        # Try to find publication date
        pub_match = re.search(date_pattern, page_text)
        if pub_match:
            dates['published'] = pub_match.group(1)
        
        # Try to find update date
        update_match = re.search(update_pattern, page_text)
        if update_match:
            dates['updated'] = update_match.group(1)
        
        # Alternative: look for specific metadata spans
        for container in date_containers:
            text = container.get_text(strip=True)
            if 'Publicado' in text:
                pub_match = re.search(r'(\d{2}/\d{2}/\d{4})', text)
                if pub_match:
                    dates['published'] = pub_match.group(1)
            if 'Atualizado' in text:
                update_match = re.search(r'(\d{2}/\d{2}/\d{4})', text)
                if update_match:
                    dates['updated'] = update_match.group(1)
        
        return dates
    
    def extract_main_content(self, soup: BeautifulSoup) -> str:
        """Extract the main content from the page"""
        # Try to find the main content area
        content_areas = [
            soup.find('main'),
            soup.find('div', {'id': 'content'}),
            soup.find('div', {'class': 'content-core'}),
            soup.find('article'),
            soup.find('div', {'id': 'parent-fieldname-text'})
        ]
        
        for area in content_areas:
            if area:
                # Remove script and style elements
                for script in area(['script', 'style']):
                    script.decompose()
                return area.get_text(separator='\n', strip=True)
        
        # Fallback: get body text
        body = soup.find('body')
        if body:
            for script in body(['script', 'style', 'nav', 'header', 'footer']):
                script.decompose()
            return body.get_text(separator='\n', strip=True)
        
        return "Content extraction failed"
    
    def get_category_links(self) -> Dict[str, str]:
        """Get links for all target categories from the main page"""
        try:
            response = self.session.get(self.base_url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            category_links = {}
            
            # Find all links on the page
            all_links = soup.find_all('a', href=True)
            
            for link in all_links:
                link_text = link.get_text(strip=True)
                href = link['href']
                
                # Check if this link matches any of our target categories
                for category in self.target_categories:
                    # Normalize text for comparison
                    normalized_link = link_text.lower().replace('\n', ' ').strip()
                    normalized_category = category.lower()
                    
                    # Check for exact or close match
                    if normalized_category in normalized_link or normalized_link in normalized_category:
                        # Build absolute URL
                        absolute_url = urljoin(self.base_url, href)
                        
                        # Only add if it's a gov.br URL and not already added
                        if 'gov.br' in absolute_url and category not in category_links:
                            category_links[category] = absolute_url
                            logging.info(f"Found link for {category}: {absolute_url}")
                            break
            
            # Alternative: Look for links in specific sections or lists
            # Check for ul/ol lists that might contain retirement types
            lists = soup.find_all(['ul', 'ol'])
            for lst in lists:
                items = lst.find_all('li')
                for item in items:
                    link = item.find('a', href=True)
                    if link:
                        link_text = link.get_text(strip=True)
                        for category in self.target_categories:
                            if category not in category_links:
                                if category.lower() in link_text.lower():
                                    absolute_url = urljoin(self.base_url, link['href'])
                                    if 'gov.br' in absolute_url:
                                        category_links[category] = absolute_url
                                        logging.info(f"Found link for {category}: {absolute_url}")
            
            return category_links
            
        except Exception as e:
            logging.error(f"Error getting category links: {str(e)}")
            return {}
    
    def scrape_page(self, url: str) -> Dict:
        """Scrape a single page and extract content and dates"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract dates
            dates = self.extract_dates(soup)
            
            # Extract main content
            content = self.extract_main_content(soup)
            
            # Get page title
            title = soup.find('title')
            title_text = title.get_text(strip=True) if title else "No title found"
            
            return {
                'url': url,
                'title': title_text,
                'published_date': dates['published'],
                'updated_date': dates['updated'],
                'content': content,  # Limit content length for display
                'content_length': len(content),
                'status': 'success'
            }
            
        except Exception as e:
            logging.error(f"Error scraping {url}: {str(e)}")
            return {
                'url': url,
                'status': 'error',
                'error': str(e)
            }
    
    def scrape_all_categories(self) -> List[Dict]:
        """Main method to scrape all retirement category pages"""
        results = []
        
        # First, get the links for all categories
        logging.info("Getting category links from main page...")
        category_links = self.get_category_links()
        
        if not category_links:
            logging.warning("No category links found. Attempting to scrape main page...")
            # If no specific links found, at least scrape the main page
            main_page_data = self.scrape_page(self.base_url)
            main_page_data['category'] = 'Main Page'
            results.append(main_page_data)
        else:
            # Scrape each category page
            for category, url in category_links.items():
                logging.info(f"Scraping {category}: {url}")
                page_data = self.scrape_page(url)
                page_data['category'] = category
                results.append(page_data)
        
        # Also check for categories not found
        found_categories = set(category_links.keys())
        missing_categories = set(self.target_categories) - found_categories
        
        for missing in missing_categories:
            results.append({
                'category': missing,
                'status': 'not_found',
                'message': f"Category '{missing}' not found on main page"
            })
        
        return results


def scrape_inss_retirement():
    """
    Azure Function to scrape INSS retirement pages
    
    Returns JSON with scraped data from all retirement categories
    """
    logging.info('Starting INSS retirement pages scraping')
    
    try:
        # Initialize scraper
        scraper = INSSRetirementScraper()
        
        # Perform scraping
        results = scraper.scrape_all_categories()
        
        # Prepare response
        response_data = {
            'status': 'success',
            'timestamp': datetime.utcnow().isoformat(),
            'total_categories': len(scraper.target_categories),
            'scraped_pages': len([r for r in results if r.get('status') == 'success']),
            'results': results
        }
        
        # Log summary
        for result in results:
            if result.get('status') == 'success':
                category = result.get('category', 'Unknown')
                updated = result.get('updated_date', 'N/A')
                logging.info(f"Successfully scraped {category} - Last updated: {updated}")
                
                # Print to console (for local testing)
                print(f"\n{'='*60}")
                print(f"Category: {category}")
                print(f"URL: {result.get('url')}")
                print(f"Title: {result.get('title')}")
                print(f"Published: {result.get('published_date', 'N/A')}")
                print(f"Updated: {updated}")
                print(f"Content Length: {result.get('content_length', 0)} characters")
                print(f"Content Preview (first 500 chars):")
                print(result.get('content', ''))
        
        return json.dumps(response_data, ensure_ascii=False, indent=2)

        
    except Exception as e:
        logging.error(f"Error in Azure Function: {str(e)}")
        error_response = {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }
        return json.dumps(error_response)

def get_latest_updated_at(df):
    latest_updated_at = df.select(F.max("scraped_at")).collect()[0][0]

    return latest_updated_at if latest_updated_at != None else '2000-01-01'

# For local testing
if __name__ == "__main__":
    print("Testing INSS Retirement Scraper locally...")
    scraper = INSSRetirementScraper()
    results = scraper.scrape_all_categories()
    
    table_df = spark.read.table("tcc_workspace_catalog.raw_inss_crawled_data.direitos_aposentadoria")
    latest_internal_updated_at = get_latest_updated_at(table_df)
    print(f"Latest internal updated_at: {latest_internal_updated_at}")
    
    new_rows = []
    for result in results:
        row = dict()
        if result.get('status') == 'success':
            row["retirement_type"] = result.get('category')
            row["page_url"] = result.get('url')
            row["html_page_content"] = result.get('content')
            row["scraped_at"] = datetime.utcnow()
            row["last_update_on_inss_site"] = datetime.strptime(result.get('updated_date') or '01/01/2000', "%d/%m/%Y")

            new_rows.append(row)
            
        else:
            print(f"\n{result.get('category', 'Unknown')}: {result.get('status')} - {result.get('message', result.get('error', ''))}")
    
    scrapped_data = spark.createDataFrame(new_rows, schema=table_df.schema)
    scrapped_data = scrapped_data.filter(F.col("last_update_on_inss_site") > latest_internal_updated_at)
    scrapped_data.display()
    scrapped_data.write.mode("append").saveAsTable("tcc_workspace_catalog.raw_inss_crawled_data.direitos_aposentadoria")
    dbutils.jobs.taskValues.set("flag_new_records_ingested", "False" if scrapped_data.isEmpty() else "True")
