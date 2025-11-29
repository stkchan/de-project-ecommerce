import argparse, os, json, random, gzip, uuid, hashlib
from datetime import datetime, timedelta
from pathlib import Path
import time
import pyarrow as pa   # type: ignore
import pyarrow.parquet as pq  # type: ignore


# --------------------------
# Utility helpers
# --------------------------
def uuid_v4_like():
    return str(uuid.uuid4())

def ga4_pseudo_id():
    return f"{random.randint(1000000, 9999999)}.{random.randint(1000000000, 9999999999)}"

def to_event_date(dt): 
    return dt.strftime("%Y%m%d")

def to_event_ts_micro(dt): 
    return str(int(dt.timestamp() * 1_000_000))

def ensure_dir(p: Path): 
    p.mkdir(parents=True, exist_ok=True)

def stable_int(s: str, mod: int): 
    return int(hashlib.sha1(s.encode()).hexdigest(), 16) % mod


# --------------------------
# Catalog setup (800 SKUs)
# --------------------------
BRANDS = [
    # Sportswear & Fashion
    "Nike", "Adidas", "Puma", "Under Armour", "Reebok", "Uniqlo", "Zara", "H&M", "Levi's", "The North Face",

    # Technology & Electronics
    "Apple", "Samsung", "Sony", "Microsoft", "Huawei", "Dell", "HP", "Lenovo", "ASUS", "Acer",

    # Automotive
    "Toyota", "Tesla", "BMW", "Mercedes-Benz", "Honda", "Ford", "Hyundai",

    # Lifestyle, Food & Home
    "IKEA", "Nestlé", "Coca-Cola", "Pepsi", "Procter & Gamble", "Unilever"
]

CATS = [
    # Clothing & Sportswear (Nike, Adidas, Puma, Reebok, Under Armour)
    ("Clothing", "Men", "Shirts"),
    ("Clothing", "Women", "Dresses"),
    ("Clothing", "Kids", "T-Shirts"),
    ("Clothing", "Men", "Sneakers"),
    ("Clothing", "Women", "Running Shoes"),
    ("Clothing", "Accessories", "Bags"),
    ("Clothing", "Accessories", "Caps"),
    ("Clothing", "Men", "Jackets"),
    ("Clothing", "Women", "Leggings"),
    ("Sports", "Outdoor", "Fitness"),
    ("Sports", "Indoor", "Yoga Mats"),
    ("Sports", "Equipment", "Soccer Balls"),
    ("Sports", "Equipment", "Tennis Rackets"),
    ("Sports", "Apparel", "Tracksuits"),
    ("Sports", "Footwear", "Basketball Shoes"),

    # Electronics & Technology (Apple, Samsung, Huawei, Sony, Microsoft, Dell, HP, Lenovo, ASUS, Acer)
    ("Electronics", "Audio", "Headphones"),
    ("Electronics", "Audio", "Wireless Earbuds"),
    ("Electronics", "Mobile", "Smartphones"),
    ("Electronics", "Mobile", "Smartwatches"),
    ("Electronics", "Computers", "Laptops"),
    ("Electronics", "Computers", "Tablets"),
    ("Electronics", "Computers", "Desktops"),
    ("Electronics", "Gaming", "Consoles"),
    ("Electronics", "Gaming", "VR Headsets"),
    ("Electronics", "Accessories", "Chargers"),
    ("Electronics", "Accessories", "Power Banks"),
    ("Electronics", "Display", "Monitors"),
    ("Electronics", "Home Entertainment", "Televisions"),
    ("Electronics", "Cameras", "Digital Cameras"),
    ("Electronics", "Networking", "WiFi Routers"),
    ("Electronics", "Smart Home", "Smart Lights"),
    ("Electronics", "Storage", "External Drives"),

    # Home & Lifestyle (IKEA, Unilever, P&G, Nestlé)
    ("Home & Kitchen", "Appliances", "Blenders"),
    ("Home & Kitchen", "Furniture", "Chairs"),
    ("Home & Kitchen", "Furniture", "Tables"),
    ("Home & Kitchen", "Decor", "Lamps"),
    ("Home & Kitchen", "Smart Home", "Smart Speakers"),
    ("Home & Kitchen", "Cleaning", "Vacuum Cleaners"),
    ("Home & Kitchen", "Storage", "Shelves"),
    ("Home & Kitchen", "Kitchenware", "Cookware Sets"),
    ("Home & Kitchen", "Bath", "Towels"),

    # Beauty & Health
    ("Beauty", "Skincare", "Moisturizers"),
    ("Beauty", "Makeup", "Lipsticks"),
    ("Beauty", "Hair Care", "Shampoo"),
    ("Beauty", "Fragrance", "Perfume"),
    ("Health", "Supplements", "Vitamins"),
    ("Health", "Devices", "Smart Scales"),
    ("Health", "Personal Care", "Electric Toothbrushes"),

    # Automotive (Toyota, Tesla, BMW, Mercedes, Ford, Honda, Hyundai)
    ("Automotive", "Accessories", "Car Chargers"),
    ("Automotive", "Accessories", "Floor Mats"),
    ("Automotive", "Maintenance", "Motor Oil"),
    ("Automotive", "Interior", "Seat Covers"),
    ("Automotive", "Navigation", "GPS Systems"),
    ("Automotive", "Electric Vehicles", "Charging Cables"),
    ("Automotive", "Safety", "Dash Cameras"),

    # Grocery, Beverages & Food (Coca-Cola, Pepsi, Nestlé)
    ("Grocery", "Beverages", "Soft Drinks"),
    ("Grocery", "Beverages", "Coffee"),
    ("Grocery", "Snacks", "Chips"),
    ("Grocery", "Snacks", "Chocolate"),
    ("Grocery", "Dairy", "Milk"),
    ("Grocery", "Breakfast", "Cereal"),

    # Books & Software (Microsoft, Sony)
    ("Books", "Education", "Textbooks"),
    ("Books", "Technology", "Programming Guides"),
    ("Software", "Office Tools", "Productivity Suite"),
    ("Software", "Entertainment", "Streaming Subscription"),
    ("Software", "Utilities", "Antivirus"),
    ("Gaming", "PC", "Video Games"),
    ("Gaming", "Console", "Controllers"),
    ("Gaming", "Online", "Game Pass Memberships")
]

VARIANTS = [
    # Apparel sizes
    "XS", "S", "M", "L", "XL", "XXL", "3XL",

    # Colors
    "Black", "White", "Red", "Blue", "Green", "Silver", "Gold", "Gray", "Pink",
    "Navy", "Beige", "Purple", "Orange", "Yellow", "Brown", "Graphite", "Midnight", "Starlight",

    # Device storage & specs
    "32GB", "64GB", "128GB", "256GB", "512GB", "1TB", "2TB",
    "8GB RAM", "16GB RAM", "32GB RAM",

    # Product tiers
    "Standard", "Pro", "Plus", "Ultra", "Max", "Mini", "Air", "Fold", "Flip",

    # Generations / Editions
    "Series 3", "Series 5", "Series 7", "Series 9",
    "Gen 1", "Gen 2", "Gen 3", "Gen 4", "Gen 5",
    "Model X", "Model Y", "Model S", "Model 3",

    # Special editions
    "Limited Edition", "Collector’s Edition", "Eco", "Sport Edition", "Premium Pack", "5G Model", "Hybrid", "EV"
]

CATALOG = [{
    "item_id": f"SKU{i:04d}",
    "item_name": f"{random.choice(BRANDS)} {random.choice(CATS)[2]} {i}",
    "item_brand": random.choice(BRANDS),
    "item_variant": random.choice(VARIANTS),
    "item_category": random.choice(CATS)[0],
    "item_category2": random.choice(CATS)[1],
    "item_category3": random.choice(CATS)[2],
    "price": round(random.uniform(10, 400), 2)
} for i in range(1, 801)]


# --------------------------
# "pick_items" function is used for generating "items" array values
# ------------------------
def pick_items():
    items = random.sample(CATALOG, random.randint(1, 4))
    for it in items:
        qty = random.randint(1, 3)
        it["quantity"] = qty
        it["item_revenue"] = round(it["price"] * qty, 2)
        it["coupon"] = random.choice([None, "WELCOME10", "NY25", "SPRING5", "HOLIDAY50", "SUMMER25", "FREESHIP", "VIP15"])
        it["affiliation"] = "Online Store"
    return items

# --------------------------
# Geo / device / campaign
# --------------------------
def geo_sample():
    return random.choice([
        ("North America", "United States", "California", "San Francisco"),
        ("North America", "United States", "California", "Los Angeles"),
        ("North America", "United States", "California", "San Diego"),
        ("North America", "United States", "New York", "New York City"),
        ("North America", "United States", "Illinois", "Chicago"),
        ("North America", "United States", "Texas", "Dallas"),
        ("North America", "United States", "Texas", "Houston"),
        ("North America", "United States", "Florida", "Miami"),
        ("North America", "United States", "Washington", "Seattle"),
        ("North America", "United States", "Massachusetts", "Boston"),
        ("North America", "United States", "Georgia", "Atlanta"),
        ("North America", "United States", "Colorado", "Denver"),
        ("North America", "United States", "Nevada", "Las Vegas"),
        ("North America", "United States", "Arizona", "Phoenix"),
        ("North America", "Canada", "Ontario", "Toronto"),
        ("North America", "Canada", "British Columbia", "Vancouver"),
        ("North America", "Mexico", "CDMX", "Mexico City"),

        ("Europe", "United Kingdom", "England", "London"),
        ("Europe", "Germany", "Bavaria", "Munich"),
        ("Europe", "Germany", "Berlin", "Berlin"),
        ("Europe", "France", "Île-de-France", "Paris"),
        ("Europe", "Italy", "Lazio", "Rome"),
        ("Europe", "Spain", "Madrid", "Madrid"),
        ("Europe", "Netherlands", "North Holland", "Amsterdam"),
        ("Europe", "Sweden", "Stockholm County", "Stockholm"),
        ("Europe", "Poland", "Mazovia", "Warsaw"),
        ("Europe", "Ireland", "Leinster", "Dublin"),
        ("Europe", "Switzerland", "Zurich", "Zurich"),
        ("Europe", "Norway", "Oslo", "Oslo"),

        ("Asia", "Thailand", "Bangkok", "Bangkok"),
        ("Asia", "Japan", "Tokyo", "Tokyo"),
        ("Asia", "Japan", "Osaka", "Osaka"),
        ("Asia", "South Korea", "Seoul", "Seoul"),
        ("Asia", "China", "Guangdong", "Shenzhen"),
        ("Asia", "China", "Beijing", "Beijing"),
        ("Asia", "Singapore", "Singapore", "Singapore"),
        ("Asia", "India", "Maharashtra", "Mumbai"),
        ("Asia", "India", "Karnataka", "Bangalore"),
        ("Asia", "Vietnam", "Ho Chi Minh City", "Ho Chi Minh City"),
        ("Asia", "Indonesia", "Jakarta", "Jakarta"),
        ("Asia", "Malaysia", "Kuala Lumpur", "Kuala Lumpur"),
        ("Asia", "Philippines", "Metro Manila", "Manila"),
        ("Asia", "Hong Kong", "Hong Kong", "Hong Kong")
    ])

def device_sample():
    return random.choice([
        {"category":"desktop","operating_system":"Windows","operating_system_version":"11"},
        {"category":"mobile","operating_system":"iOS","operating_system_version":"17"},
        {"category":"mobile","operating_system":"Android","operating_system_version":"14"}
    ])

def base_campaign():
    paid = random.random() < 0.6
    if paid:
        return {
            "campaign_id": "CMP2025",
            "campaign_name": random.choice([
                "HolidayPromo2025", "SummerBlitz2025", "BackToSchool2025",
                "SpringSale2025", "WinterDeals2025", "MegaDiscountWeek",
                "BlackFriday2025", "CyberMonday2025", "YearEndClearance",
                "NewArrivalsLaunch"
            ]),
            "source": random.choice([
                "google", "facebook", "instagram", "tiktok", "twitter",
                "linkedin", "newsletter", "youtube", "bing", "pinterest"
            ]),
            "medium": random.choice([
                "cpc", "email", "social", "affiliate", "display",
                "referral", "push", "video", "organic_social"
            ]),
            "term": random.choice([
                "winter+sale", "gift+ideas", "holiday+offers", "smartphones+discount",
                "fashion+sale", "fitness+gear", "home+decor", "beauty+skincare",
                "electronics+deal", "back+to+school", "kitchen+appliances",
                "gaming+console", "furniture+discount"
            ]),
            "content": random.choice([
                "header_banner", "cta_button", "sidebar_ad", "carousel_ad",
                "promo_popup", "video_ad", "email_footer", "sms_link",
                "story_swipe", "homepage_feature"
            ]),
            "gclid": f"Cj0K{random.randint(100000,999999)}"
        }
    else:
        return {
            "campaign_id": "ORG001",
            "campaign_name": random.choice([
                "OrganicSEO", "DirectTraffic", "BlogReferral", "BrandAwareness"
            ]),
            "source": random.choice([
                "google", "bing", "yahoo", "direct", "medium_blog"
            ]),
            "medium": random.choice([
                "organic", "referral", "(none)"
            ]),
            "term": "(not provided)",
            "content": "(not set)",
            "gclid": None
        }

# --------------------------
# Event generation logic
# --------------------------
FUNNEL = [
    ("first_visit","Home","/"),
    ("session_start","Home","/"),
    ("page_view","Home","/"),
    ("scroll","Home","/details"),
    ("view_promotion","Home","/"),
    ("view_search_results","Search","/search?q=jackets"),
    ("select_item","Products","/products"),
    ("view_item","Product","/product"),
    ("add_to_cart","Cart","/cart"),
    ("view_cart","Cart","/cart"),
    ("remove_from_cart","Cart","/cart"),
    ("begin_checkout","Checkout","/checkout"),
    ("add_payment_info","Checkout Payment","/checkout/payment"),
    ("purchase","Confirmation","/confirmation")
]

# -----------------------------------
# Event creation
# -----------------------------------
def make_event(ename, dt, user_id, pseudo, session_id, campaign, geo, device, page_title, url, items=None, params=None):
    evp = params or {}
    evp["ga_session_id"] = session_id
    evp["page_title"] = page_title
    evp["page_location"] = f"https://shop.mundo.com{url}"
    return {
        "event_date": to_event_date(dt),
        "event_timestamp": to_event_ts_micro(dt),
        "event_name": ename,
        "user_id": user_id,
        "user_pseudo_id": pseudo,
        "geo": json.dumps({"continent":geo[0],"country":geo[1],"region":geo[2],"city":geo[3]}),
        "device": json.dumps(device),
        "traffic_source": json.dumps(campaign),
        "event_params": json.dumps(evp),
        "items": json.dumps(items or [])
    }

# -----------------------------------
# Chunked generation
# -----------------------------------
def generate_day(out_dir, date_dt, min_events, max_events, chunk_size=100_000):
    # Random how many events to generate
    ensure_dir(out_dir)
    day_str     = to_event_date(date_dt)
    target      = random.randint(min_events, max_events)
    output_path = Path(out_dir) / f"ga4data_{day_str}.parquet"
    print(f"📅 Generating {target:,} events for {day_str} ...")

    # Specify schema of data
    schema = pa.schema([
        ("event_date", pa.string()),
        ("event_timestamp", pa.string()),
        ("event_name", pa.string()),
        ("user_id", pa.string()),
        ("user_pseudo_id", pa.string()),
        ("geo", pa.string()),
        ("device", pa.string()),
        ("traffic_source", pa.string()),
        ("event_params", pa.string()),
        ("items", pa.string())
    ])

    writer = pq.ParquetWriter(output_path, schema, compression="snappy")
    total = 0
    start = time.time()

    # Loop in chunks for generating data
    while total < target:
        batch = []
        for _ in range(min(chunk_size, target - total)):
            pseudo    = ga4_pseudo_id()
            session   = random.randint(10_000_000, 99_999_999)
            geo       = geo_sample()
            device    = device_sample()
            campaign  = base_campaign()
            cart      = pick_items()
            logged_in = (stable_int(pseudo, 100) < 35)
            user_id   = uuid_v4_like() if logged_in else None
            base_time = datetime(date_dt.year, date_dt.month, date_dt.day,
                                 random.randint(8, 22), random.randint(0, 59))
            steps     = FUNNEL[:random.randint(3, len(FUNNEL))]

            for idx, (ename, title, url) in enumerate(steps):
                dt = base_time + timedelta(minutes=idx * 2)
                items_to_use = [] if ename in {"first_visit","page_view"} else [dict(it) for it in cart]
                ev = make_event(ename, dt, user_id, pseudo, session, campaign, geo, device, title, url, items_to_use)
                batch.append(ev)

        # Convert to table and write chunk
        table = pa.Table.from_pylist(batch, schema=schema)
        writer.write_table(table)
        total += len(batch)
        print(f"✅ Wrote chunk: {len(batch):,} events  (Total: {total:,})")

    writer.close()
    elapsed = time.time() - start
    mins, secs = divmod(elapsed, 60)
    print(f"✅ Finished {day_str}: {total:,} events → {output_path}")
    print(f"⏱️ Generation time: {mins:.0f}m {secs:.0f}s\n")

    # return metrics for __main__
    return total, elapsed, str(output_path)

# -----------------------------------
# CLI entry
# -----------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", help="YYYYMMDD")
    ap.add_argument("--min-events", type=int, default=100)
    ap.add_argument("--max-events", type=int, default=200)
    ap.add_argument("--out-dir", default="./data/ga4")
    args = ap.parse_args()

    date_dt = datetime.strptime(args.start_date, "%Y%m%d") if args.start_date else datetime.now() - timedelta(days=1)
    print(f"🕓 Generating GA4 Parquet for {to_event_date(date_dt)} ...")

    start_time = time.time()
    total_rows, generation_time, out_file = generate_day(Path(args.out_dir), date_dt, args.min_events, args.max_events)
    elapsed = round(time.time() - start_time, 2)

    metrics_path = Path(args.out_dir) / "last_run_metrics.json"
    with open(metrics_path, "w") as f:
        json.dump({
            "event_date": to_event_date(date_dt),
            "file_path": out_file,
            "total_rows": total_rows,
            "generation_time": round(elapsed, 2)
        }, f)

    print(f"📈 Saved metrics → {metrics_path}")

