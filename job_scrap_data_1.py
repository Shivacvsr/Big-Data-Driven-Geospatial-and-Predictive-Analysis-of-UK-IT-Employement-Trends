import json
import boto3
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import time
import re
from io import StringIO
import os
import psycopg2
from psycopg2.extras import execute_batch
from time import sleep
from typing import Tuple, Optional

class LinkedInRecentITJobsScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }
        
        self.it_jobs = [
            "Backend Developer",
            "Frontend Developer",
            "Machine Learning Engineer"
        ]

        # Add database configuration
        self.db_config = {
            'dbname': os.environ['DB_NAME'],
            'user': os.environ['DB_USER'],
            'password': os.environ['DB_PASSWORD'],
            'host': os.environ['DB_HOST'],
            'port': os.environ['DB_PORT']
        }

        self.geocoding_cache = {}  # Simple in-memory cache

    def clean_location(self, location):
        """Clean location string to extract only city name"""
        location = re.sub(r', England, United Kingdom$', '', location)
        location = re.sub(r', United Kingdom$', '', location)
        location = re.sub(r', UK$', '', location)
        location = re.sub(r' Area, United Kingdom$', '', location)
        location = re.sub(r' Area$', '', location)
        location = re.sub(r'Greater ', '', location)
        
        if ',' in location:
            location = location.split(',')[0].strip()
            
        return location

    def determine_experience_level(self, job_description):
        """Determine if the job is entry-level, mid-level, or senior"""
        text = job_description.lower()
        
        if any(word in text for word in ['senior', 'lead', 'principal', 'manager', 'head of']):
            return 'Senior Level'
        elif any(word in text for word in ['junior', 'entry', 'graduate', 'trainee']):
            return 'Entry Level'
        else:
            return 'Mid Level'

    def is_remote(self, job_description):
        """Check if the job is remote"""
        text = job_description.lower()
        if any(word in text for word in ['remote', 'work from home', 'wfh', 'hybrid']):
            if 'hybrid' in text:
                return 'Hybrid'
            return 'Remote'
        return 'On-site'

    def is_recent_job(self, posted_date_text):
        """Check if the job was posted recently (within last 3 days)"""
        text = posted_date_text.lower()
        
        if 'hours ago' in text or 'hour ago' in text:
            return True
        elif 'day ago' in text or '1 day ago' in text:
            return True
        elif '2 days ago' in text:
            return True
        elif '3 days ago' in text:
            return True
        
        return False

    def get_coordinates(self, location: str) -> Tuple[Optional[float], Optional[float]]:
        """Get latitude and longitude for a location using Nominatim (OpenStreetMap)"""
        if not location:
            return None, None
            
        # Check cache first
        if location in self.geocoding_cache:
            return self.geocoding_cache[location]
            
        try:
            # Add UK to the location query to improve accuracy
            search_location = f"{location}, United Kingdom"
            
            # Using Nominatim API
            url = "https://nominatim.openstreetmap.org/search"
            headers = {
                'User-Agent': 'LinkedInJobScraper/1.0',  # Required by Nominatim
                'Accept-Language': 'en-US,en;q=0.9',
            }
            params = {
                'q': search_location,
                'format': 'json',
                'limit': 1
            }
            
            response = requests.get(url, headers=headers, params=params)
            sleep(1)  # Respect rate limit - 1 request per second
            
            if response.status_code == 200:
                results = response.json()
                if results:
                    lat = float(results[0]['lat'])
                    lon = float(results[0]['lon'])
                    # Cache the result
                    self.geocoding_cache[location] = (lat, lon)
                    return lat, lon
            
            # Cache negative result
            self.geocoding_cache[location] = (None, None)
            return None, None
            
        except Exception as e:
            print(f"Error geocoding location '{location}': {str(e)}")
            return None, None

    def scrape_linkedin_jobs(self):
        """Scrape recent IT jobs from LinkedIn UK"""
        all_jobs = []
        max_pages_per_category = 1  # Reduced for Lambda execution time constraints
        
        for job_title in self.it_jobs:
            print(f"\nScraping recent jobs for: {job_title}")
            page = 0
            consecutive_old_jobs = 0
            
            while page < max_pages_per_category:
                try:
                    encoded_title = requests.utils.quote(job_title)
                    url = f"https://www.linkedin.com/jobs/search/?keywords={encoded_title}&location=United%20Kingdom&start={page*25}&f_TPR=r86400"
                    print(f"Scraping page {page + 1}...")
                    
                    response = requests.get(url, headers=self.headers)
                    if response.status_code != 200:
                        print(f"Failed to fetch page {page + 1}. Status code: {response.status_code}")
                        break
                    
                    soup = BeautifulSoup(response.text, 'html.parser')
                    job_cards = soup.find_all('div', class_='base-card')
                    
                    if not job_cards:
                        print("No more jobs found on this page")
                        break
                    
                    recent_jobs_found = False
                    
                    for job in job_cards:
                        try:
                            posted_date = job.find('time', class_='job-search-card__listdate')
                            if posted_date:
                                posted_date_text = posted_date.text.strip()
                                if not self.is_recent_job(posted_date_text):
                                    continue
                                
                            recent_jobs_found = True
                            
                            title = job.find('h3', class_='base-search-card__title').text.strip()
                            company = job.find('h4', class_='base-search-card__subtitle').text.strip()
                            raw_location = job.find('span', class_='job-search-card__location').text.strip()
                            location = self.clean_location(raw_location)
                            job_url = job.find('a', class_='base-card__full-link')['href']
                            
                            job_response = requests.get(job_url, headers=self.headers)
                            job_soup = BeautifulSoup(job_response.text, 'html.parser')
                            job_description = job_soup.find('div', class_='show-more-less-html__markup').text.strip() if job_soup.find('div', class_='show-more-less-html__markup') else ""
                            
                            experience_level = self.determine_experience_level(job_description)
                            work_type = self.is_remote(job_description)
                            
                            all_jobs.append({
                                'Job Title': title,
                                'Company': company,
                                'Location': location,
                                'Experience Level': experience_level,
                                'Work Type': work_type,
                                'Category': job_title,
                                'Posted Date': posted_date_text if posted_date else 'Recently',
                                'Job URL': job_url,
                                'Date Scraped': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            })
                            
                        except Exception as e:
                            print(f"Error parsing job: {str(e)}")
                            continue
                    
                    if not recent_jobs_found:
                        consecutive_old_jobs += 1
                        if consecutive_old_jobs >= 2:
                            break
                    else:
                        consecutive_old_jobs = 0
                    
                    page += 1
                    time.sleep(1)  # Reduced sleep time for Lambda
                    
                except Exception as e:
                    print(f"Error fetching page: {str(e)}")
                    break
        
        return all_jobs

    def save_to_s3(self, jobs):
        """Save jobs to CSV file in S3"""
        if not jobs:
            print("No jobs to save")
            return
        
        try:
            # Create CSV in memory
            csv_buffer = StringIO()
            fieldnames = ['Job Title', 'Company', 'Location', 'Latitude', 'Longitude', 
                         'Experience Level', 'Work Type', 'Category', 'Posted Date', 
                         'Job URL', 'Date Scraped']
            
            # Add coordinates to jobs data
            jobs_with_coordinates = []
            for job in jobs:
                job_copy = job.copy()
                lat, lng = self.get_coordinates(job['Location'])
                job_copy['Latitude'] = lat
                job_copy['Longitude'] = lng
                jobs_with_coordinates.append(job_copy)
                time.sleep(0.1)  # Rate limiting
            
            writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(jobs_with_coordinates)
            
            # Upload to S3
            s3 = boto3.client('s3')
            bucket_name = os.environ['S3_BUCKET_NAME']
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f'linkedin_recent_it_jobs_{timestamp}.csv'
            
            s3.put_object(
                Bucket=bucket_name,
                Key=file_name,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            
            print(f"\nSaved {len(jobs)} jobs to S3: {bucket_name}/{file_name}")
            return f"s3://{bucket_name}/{file_name}"
        
        except Exception as e:
            print(f"Error saving to S3: {str(e)}")
            return None

    def save_to_postgres(self, jobs):
        """Save jobs to PostgreSQL database"""
        if not jobs:
            print("No jobs to save to database")
            return

        conn = None
        cur = None
        try:
            print("Connecting to database...")
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            # Modified table creation to include latitude and longitude
            create_table_query = """
            CREATE TABLE IF NOT EXISTS linkedin_jobs (
                id SERIAL PRIMARY KEY,
                job_title VARCHAR(255),
                company VARCHAR(255),
                location VARCHAR(255),
                latitude DECIMAL(10, 8),
                longitude DECIMAL(11, 8),
                experience_level VARCHAR(50),
                work_type VARCHAR(50),
                category VARCHAR(100),
                posted_date VARCHAR(100),
                job_url TEXT,
                date_scraped TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cur.execute(create_table_query)
            
            # Add unique constraint if it doesn't exist
            try:
                cur.execute("""
                    ALTER TABLE linkedin_jobs 
                    ADD CONSTRAINT unique_job_url UNIQUE (job_url);
                """)
            except psycopg2.errors.DuplicateTable:
                conn.rollback()
            
            conn.commit()
            
            insert_query = """
                INSERT INTO linkedin_jobs 
                (job_title, company, location, latitude, longitude, experience_level, work_type, 
                 category, posted_date, job_url, date_scraped)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_url) DO NOTHING
            """
            
            # Modify job data to include coordinates
            job_data = []
            for job in jobs:
                lat, lng = self.get_coordinates(job['Location'])
                job_data.append((
                    job['Job Title'],
                    job['Company'],
                    job['Location'],
                    lat,
                    lng,
                    job['Experience Level'],
                    job['Work Type'],
                    job['Category'],
                    job['Posted Date'],
                    job['Job URL'],
                    datetime.strptime(job['Date Scraped'], "%Y-%m-%d %H:%M:%S")
                ))
                # Add a small delay to avoid hitting API rate limits
                time.sleep(0.1)
            
            execute_batch(cur, insert_query, job_data)
            conn.commit()
            
            print(f"\nSaved {len(jobs)} jobs to PostgreSQL database")
            
        except Exception as e:
            print(f"Error saving to PostgreSQL: {str(e)}")
            if conn:
                conn.rollback()
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

def lambda_handler(event, context):
    try:
        print("Starting LinkedIn job scraper...")
        scraper = LinkedInRecentITJobsScraper()
        jobs = scraper.scrape_linkedin_jobs()
        
        # Save to S3
        s3_path = scraper.save_to_s3(jobs)
        
        # Save to PostgreSQL
        scraper.save_to_postgres(jobs)
        
        # Prepare summary
        categories = {}
        for job in jobs:
            category = job['Category']
            if category not in categories:
                categories[category] = 0
            categories[category] += 1
        
        summary = {
            'total_jobs': len(jobs),
            'jobs_by_category': categories,
            'output_file': s3_path,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        return {
            'statusCode': 200,
            'body': json.dumps(summary)
        }
        
    except Exception as e:
        print(f"Error in lambda execution: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

