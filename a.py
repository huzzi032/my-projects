from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import time
from datetime import datetime
import logging
import re
import socket
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from queue import Queue
from threading import Lock

# Step 1: Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Step 2: Function to check if port is available
def is_port_available(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) != 0

# Step 3: Find an available port
def find_available_port(start_port=9515):
    port = start_port
    while not is_port_available(port):
        port += 1
        if port > start_port + 100:  # Limit search to avoid infinite loop
            raise Exception("No available ports found in range")
    return port

# Step 4: Configure requests with a large connection pool and retries
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries, pool_maxsize=100, pool_connections=100)
session.mount('http://', adapter)
session.mount('https://', adapter)

# Step 5: Global lock and queue for managing requests
request_queue = Queue(maxsize=50)
request_lock = Lock()

# Step 6: Set up Selenium with ChromeDriver (dynamic port)
chromedriver_path = r"E:\abdullah\chromedriver-win64\chromedriver.exe"  # Replace with your ChromeDriver path
try:
    port = find_available_port()
    service = Service(chromedriver_path, port=port)
except Exception as e:
    logging.error(f"Error finding available port: {e}")
    exit(1)

options = webdriver.ChromeOptions()
options.add_argument("--headless")  # Run in headless mode for efficiency
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-blink-features=AutomationControlled")  # Avoid detection as bot
options.add_argument("--window-size=1920,1080")  # Ensure larger viewport for scrolling

# Step 7: Initialize driver with retry logic
max_driver_attempts = 3
driver = None
for attempt in range(max_driver_attempts):
    try:
        driver = webdriver.Chrome(service=service, options=options)
        break
    except Exception as e:
        logging.error(f"Attempt {attempt + 1}/{max_driver_attempts} to start ChromeDriver failed: {e}")
        time.sleep(5)
if not driver:
    logging.critical("Failed to initialize ChromeDriver after all attempts. Exiting.")
    exit(1)

# Step 8: Define all 12 cities with sample postal codes
cities = {
    "Madrid": "28001",
    "Barcelona": "08001",
    "Valencia": "46001",
    "Malaga": "29001",
    "Sevilla": "41001",
    "Zaragoza": "50001",
    "Valladolid": "47001",
    "Segovia": "40001",
    "Murcia": "30001",
    "Cartagena": "30201",
    "La Coruña": "15001",
    "Bilbao": "48001"
}

# Step 9: Define all 200 categories (full list for starting from scratch)
categories = [
    "Butcher shop", "Natural products store", "Fishmonger", "Fruit shop", "Florist", "Jewelry", "Pastry shop",
    "Gourmet store", "Delicatessen", "Fruit juice bar", "Café", "Bakery", "Rope shop of fruits", "Zapatillas",
    "Stationery", "Toys", "Perfumery", "Cosmetics", "Liquor store", "Winery", "Organic products store",
    "Boutique", "Clothing store", "Shoe store", "Sports store", "Electronics store", "Furniture store",
    "Hardware store", "Pet store", "Bookstore", "Music store", "Video game store", "Toy store", "Gift shop",
    "Souvenir shop", "Hobby store", "Craft store", "Art supplies store", "Party supplies store", "Office supplies store",
    "Stationery store", "Computer store", "Phone store", "Camera store", "Photography studio", "Printing shop",
    "Copy shop", "Jewelry store", "Watch store", "Optical store", "Toy library", "Game store", "Board game store",
    "Puzzle store", "Rope shop of toys", "Clothing rental", "Costume rental", "Formal wear rental", "Baby store",
    "Maternity store", "Children’s clothing store", "Toy rental", "Book rental", "Movie rental", "Music rental",
    "Video rental", "Game rental", "Library", "Cultural center", "Art gallery", "Museum", "Theater", "Cinema",
    "Concert hall", "Dance academy", "Music academy", "Art academy", "Drama academy", "Painting academy",
    "Sculpture academy", "Photography academy", "Fashion academy", "Design academy", "Interior design academy",
    "Graphic design academy", "Web design academy", "Marketing academy", "Advertising agency", "Public relations agency",
    "Event planning agency", "Wedding planner", "Party planner", "Catering service", "Travel agency", "Tour operator",
    "Hotel", "Hostel", "Apartment rental", "Vacation rental", "Bed and breakfast", "Camping", "Car rental",
    "Bike rental", "Scooter rental", "Beauty salon", "Hair salon", "Barbershop", "Nail salon", "Spa", "Massage parlor",
    "Tattoo parlor", "Piercing parlor", "Gym", "Fitness center", "Yoga studio", "Pilates studio", "Dance studio",
    "Martial arts school", "Boxing club", "Kickboxing club", "Judo club", "Karate club", "Taekwondo club",
    "Swimming school", "Diving school", "Surfing school", "Sailing school", "Tennis club", "Golf club", "Riding school",
    "Skating rink", "Ice skating rink", "Bowling alley", "Billiards hall", "Arcade", "Escape room", "Paintball field",
    "Laser tag arena", "Trampoline park", "Climbing wall", "Language school", "Driving school", "Cooking school",
    "Baking school", "Bartending school", "Barista school", "Sommelier school", "Nutrition school", "Personal training",
    "Life coaching", "Business coaching", "Career counseling", "Therapy center", "Psychology center", "Psychiatry center",
    "Dentistry", "Orthodontics", "Pediatrics", "Gynecology", "Ophthalmology", "Dermatology", "Cardiology", "Neurology",
    "Pharmacy", "Hospital", "Clinic", "Rehabilitation center", "Physical therapy", "Chiropractic", "Acupuncture",
    "Naturopathy", "Homeopathy", "Veterinary clinic", "Pet grooming", "Pet boarding", "Pet training", "Kennels",
    "Auto repair", "Car wash", "Tire shop", "Oil change", "Auto body shop", "Auto parts store", "Motorcycle repair",
    "Bicycle repair", "Scooter repair", "Boat repair", "RV repair", "Trailer repair", "Truck repair", "Heavy machinery repair",
    "Construction equipment repair", "Gardening service", "Landscaping service", "Pest control", "Cleaning service",
    "Housekeeping", "Laundry service", "Dry cleaning", "Ironing service", "Tailoring", "Sewing service", "Embroidery service",
    "Knitting service", "Crochet service", "Accounting firm", "Tax service", "Financial advisor", "Insurance agency",
    "Legal service", "Notary", "Real estate agency", "Property management", "Architectural firm", "Engineering firm",
    "Construction company", "Plumbing service", "Electrical service", "HVAC service", "Roofing service", "Painting service",
    "Carpentry service", "Masonry service", "Tiling service", "Flooring service", "Insulation service", "Waterproofing service"
]

# Step 10: Define output columns
columns = [
    "Category", "Business name", "Street", "Number", "Postal code", "City",
    "Phone 1", "Phone 2", "Mobile 1", "Mobile 2", "Mail", "Web url",
    "Instagram", "Facebook", "TikTok", "Linkedin",
    "Business hours Monday", "Business hours Tuesday", "Business hours Wednesday",
    "Business hours Thursday", "Business hours Friday", "Business hours Saturday",
    "Business hours Sunday", "Latitude", "Longitude", "Main image of the business"
]

# Step 11: Initialize data list
all_data = []

# Step 12: Function to download image with improved handling, retry logic, and queue management
def download_image(url, business_name, category, city, max_retries=3):
    if not url or "http" not in url:
        logging.warning(f"No valid image URL for {business_name}")
        return None
    folder = f"images_{city}"
    os.makedirs(folder, exist_ok=True)
    # Sanitize business name to remove or replace invalid characters
    sanitized_name = re.sub(r'[\|/\\:<>*?"\']', '_', business_name)  # Replace special chars with underscore
    sanitized_name = sanitized_name.replace(' ', '_')  # Ensure spaces are underscores
    filename = f"{folder}/{category}_{sanitized_name}.jpg"
    retries = 0
    while retries < max_retries:
        try:
            with request_lock:
                while request_queue.full():
                    time.sleep(0.1)  # Very short wait to minimize delays
                request_queue.put(1)
            with session.get(url, timeout=30) as response:  # Increased timeout to 30 seconds
                if response.status_code == 200 and 'image' in response.headers.get('Content-Type', ''):
                    with open(filename, 'wb') as f:
                        f.write(response.content)
                    logging.info(f"Downloaded image for {business_name} to {filename}")
                    return filename
                else:
                    logging.warning(f"Failed to download {url}: Not an image or bad response ({response.status_code})")
        except Exception as e:
            logging.error(f"Error downloading image for {business_name} (attempt {retries + 1}/{max_retries}): {e}")
        finally:
            with request_lock:
                request_queue.get()
        retries += 1
        time.sleep(0.5)  # Reduced wait before retry for speed
    return None

# Step 13: Function to scrape website for email and social media with queue management and timeout handling
def scrape_website(url):
    socials = {"Instagram": "", "Facebook": "", "TikTok": "", "Linkedin": ""}
    email = ""
    if not url or "http" not in url:
        return email, socials
    try:
        with request_lock:
            while request_queue.full():
                time.sleep(0.1)  # Very short wait to minimize delays
            request_queue.put(1)
        with session.get(url, timeout=30) as response:  # Increased timeout to 30 seconds
            soup = BeautifulSoup(response.content, "html.parser")
            for text in soup.find_all(string=True):
                if re.search(r'[\w\.-]+@[\w\.-]+', text):
                    email = re.search(r'[\w\.-]+@[\w\.-]+', text).group()
                    break
            for a in soup.find_all("a", href=True):
                href = a["href"].lower()
                if "instagram.com" in href and not socials["Instagram"]:
                    socials["Instagram"] = href
                elif "facebook.com" in href and not socials["Facebook"]:
                    socials["Facebook"] = href
                elif "tiktok.com" in href and not socials["TikTok"]:
                    socials["TikTok"] = href
                elif "linkedin.com" in href and not socials["Linkedin"]:
                    socials["Linkedin"] = href
    except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout, requests.exceptions.RequestException) as e:
        logging.warning(f"Timeout or connection error for {url}: {e}")
        return "", socials  # Skip and return empty data
    except Exception as e:
        logging.error(f"Failed to scrape {url}: {e}")
    finally:
        with request_lock:
            request_queue.get()
    return email, socials

# Step 14: Function to scrape a single business with improved element handling, timeouts, and retries
def scrape_business(category, city, postal_code, driver_instance, max_attempts=3, max_wait=45, max_execution_time=300):
    data = []
    search_query = f"{category} near {postal_code} {city} Spain"
    attempt = 0
    start_time = time.time()
    while attempt < max_attempts and (time.time() - start_time) < max_execution_time:
        try:
            driver_instance.get("https://www.google.com/maps")
            time.sleep(3)

            # Search with longer wait and timeout
            search_box = WebDriverWait(driver_instance, max_wait).until(
                EC.presence_of_element_located((By.ID, "searchboxinput")),
                message=f"Timeout waiting for search box for {category} in {city}"
            )
            search_box.clear()
            search_box.send_keys(search_query)
            search_box.send_keys(Keys.ENTER)
            time.sleep(5)

            # Wait for results to be visible and scroll with improved XPath and timeout
            try:
                # Updated XPaths to handle potential Google Maps changes
                scrollable_options = [
                    "//div[contains(@aria-label, 'Results')]",
                    "//div[contains(@aria-label, 'Resultados')]",  # Spanish version
                    "//div[contains(@class, 'section-scrollbox')]"  # Fallback class
                ]
                scrollable_found = False
                for xpath in scrollable_options:
                    try:
                        scrollable = WebDriverWait(driver_instance, max_wait).until(
                            EC.presence_of_element_located((By.XPATH, xpath)),
                            message=f"Timeout waiting for results for {category} in {city} with XPath {xpath}"
                        )
                        scrollable_found = True
                        break
                    except:
                        continue
                if not scrollable_found:
                    logging.warning(f"Couldn’t find scrollable results in {city} for {category} after trying multiple XPaths")
                    return []  # Skip this category if no results can be scrolled

                for _ in range(3):
                    driver_instance.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable)
                    time.sleep(2)
            except Exception as e:
                logging.warning(f"Couldn’t scroll results in {city} for {category}: {e}")
                continue  # Retry or skip if scrolling fails

            # Extract businesses
            businesses = driver_instance.find_elements(By.CLASS_NAME, "hfpxzc")
            if not businesses:
                logging.warning(f"No businesses found for {category} in {city}")
                return []

            for business in businesses[:5]:  # Limit to 5 for test (adjust as needed)
                try:
                    business_name = business.get_attribute("aria-label")
                    business.click()
                    time.sleep(3)

                    details_pane = WebDriverWait(driver_instance, max_wait).until(
                        EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'm6QErb')]")),
                        message=f"Timeout waiting for details pane for {business_name}"
                    )

                    # Address
                    address = details_pane.find_element(By.XPATH, "//button[@data-item-id='address']").text
                    street = number = postal = city_name = ""
                    parts = address.split(", ")
                    if len(parts) > 1:
                        street_parts = parts[0].split(" ")
                        number = street_parts[0] if street_parts[0].isdigit() else ""
                        street = " ".join(street_parts[1:]) if number else parts[0]
                        postal = parts[-2] if parts[-2].isdigit() else postal_code
                        city_name = city

                    # Phone
                    phone = ""
                    try:
                        phone_elem = details_pane.find_element(By.XPATH, "//button[contains(@data-item-id, 'phone')]")
                        phone = phone_elem.text
                    except:
                        pass

                    # Website
                    website = ""
                    try:
                        website_elem = details_pane.find_element(By.XPATH, "//a[@data-item-id='authority']")
                        website = website_elem.get_attribute("href")
                    except:
                        pass

                    # Scrape website for email and social media
                    email, socials = scrape_website(website)

                    # Hours
                    hours_dict = {day: "" for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]}
                    try:
                        hours_table = details_pane.find_element(By.XPATH, "//table[contains(@class, 'y0skZc')]")
                        rows = hours_table.find_elements(By.TAG_NAME, "tr")
                        for row in rows:
                            day = row.find_element(By.XPATH, ".//td[1]").text.split(" ")[0]
                            hours = row.find_element(By.XPATH, ".//td[2]").text
                            hours_dict[day] = hours
                    except:
                        pass

                    # Image with queue management and timeout
                    image_url = ""
                    downloaded_image = None
                    try:
                        try:
                            photos_tab = driver_instance.find_element(By.XPATH, "//button[contains(@aria-label, 'Photos')]")
                            photos_tab.click()
                            time.sleep(2)
                        except:
                            pass
                        
                        image_elem = WebDriverWait(driver_instance, max_wait // 2).until(
                            EC.presence_of_element_located((By.XPATH, "//img[contains(@class, 'gallery-image') or contains(@alt, 'Photo of')]")),
                            message=f"Timeout waiting for image for {business_name}"
                        )
                        image_url = image_elem.get_attribute("src")
                        if image_url:
                            downloaded_image = download_image(image_url, business_name, category, city)
                    except Exception as e:
                        logging.warning(f"Failed to find image for {business_name}: {e}")
                        try:
                            fallback_image = driver_instance.find_element(By.XPATH, "//img[@decoding='async']")
                            image_url = fallback_image.get_attribute("src")
                            downloaded_image = download_image(image_url, business_name, category, city)
                        except:
                            pass

                    # Coordinates
                    latitude = longitude = ""
                    try:
                        current_url = driver_instance.current_url
                        if "@" in current_url:
                            coords = current_url.split("@")[1].split(",")[0:2]
                            latitude, longitude = coords[0], coords[1]
                    except:
                        pass

                    # Add to data
                    data.append({
                        "Category": category,
                        "Business name": business_name,
                        "Street": street,
                        "Number": number,
                        "Postal code": postal,
                        "City": city_name,
                        "Phone 1": phone,
                        "Phone 2": "",
                        "Mobile 1": "",
                        "Mobile 2": "",
                        "Mail": email,
                        "Web url": website,
                        "Instagram": socials["Instagram"],
                        "Facebook": socials["Facebook"],
                        "TikTok": socials["TikTok"],
                        "Linkedin": socials["Linkedin"],
                        "Business hours Monday": hours_dict["Monday"],
                        "Business hours Tuesday": hours_dict["Tuesday"],
                        "Business hours Wednesday": hours_dict["Wednesday"],
                        "Business hours Thursday": hours_dict["Thursday"],
                        "Business hours Friday": hours_dict["Friday"],
                        "Business hours Saturday": hours_dict["Saturday"],
                        "Business hours Sunday": hours_dict["Sunday"],
                        "Latitude": latitude,
                        "Longitude": longitude,
                        "Main image of the business": downloaded_image or image_url or ""
                    })

                except Exception as e:
                    logging.error(f"Error processing {business_name} in {city} for {category}: {e}")
                    continue

            return data

        except Exception as e:
            logging.error(f"Attempt {attempt + 1}/{max_attempts} failed for {category} in {city}: {e}")
            attempt += 1
            if attempt < max_attempts:
                time.sleep(5)
            else:
                return []
    logging.warning(f"Max execution time ({max_execution_time} seconds) exceeded for {category} in {city}")
    return []

# Step 15: Sequential execution with periodic saving, graceful interruption, and auto-continuation
def main():
    global all_data
    save_interval = 10  # Save every 10 categories
    categories_processed = 0
    last_save_time = time.time()

    try:
        for city in cities.keys():
            for category in categories:
                try:
                    data = scrape_business(category, city, cities[city], driver, max_execution_time=300)
                    all_data.extend(data)
                    categories_processed += 1
                    time.sleep(1)  # Small delay between tasks to reduce request pressure

                    # Save periodically (every 10 categories or every 30 minutes)
                    if categories_processed % save_interval == 0 and all_data:
                        save_data(f"business_data_partial_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
                        logging.info(f"Partial data saved after {categories_processed} categories")

                    # Save every 30 minutes regardless of category count
                    if time.time() - last_save_time >= 1800:  # 30 minutes in seconds
                        save_data(f"business_data_partial_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
                        logging.info(f"Partial data saved after 30 minutes")
                        last_save_time = time.time()

                except Exception as e:
                    logging.error(f"Error processing {category} in {city}: {e}")
                    continue  # Skip to the next category/city if an error occurs, ensuring continuation

        # Final save
        if all_data:
            save_data(f"business_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
            logging.info(f"Final data saved to Excel")
        else:
            logging.warning("No data collected to save.")

    except KeyboardInterrupt:
        # Save on Ctrl + C to prevent data loss
        if all_data:
            save_data(f"business_data_interrupted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
            logging.info(f"Interrupted data saved to Excel")
        else:
            logging.warning("No data collected to save on interruption.")
        if driver:
            driver.quit()
        exit(0)

    finally:
        # Ensure driver is quit and final save occurs even if an error happens
        if all_data:
            save_data(f"business_data_final_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
            logging.info(f"Final data saved on script completion or error")
        if driver:
            driver.quit()

# Step 16: Helper function to save data to Excel
def save_data(filename):
    df = pd.DataFrame(all_data, columns=columns)
    df.to_excel(filename, index=False, engine='openpyxl')

if __name__ == "__main__":
    main()