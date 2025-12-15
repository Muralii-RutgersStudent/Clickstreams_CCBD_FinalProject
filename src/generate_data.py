import json
import random
import time
from datetime import datetime, timedelta
import os

# Configuration
NUM_USERS = 100
TOTAL_EVENTS = 5000
OUTPUT_FILE = "data/clickstream_data.json"

# Funnel stages
PAGES = [
    "home",
    "search",
    "product_view",
    "add_to_cart",
    "checkout",
    "confirmation"
]

# User behavior probabilities (simple Markov chain-like)
TRANSITIONS = {
    "home": {"search": 0.4, "product_view": 0.4, "home": 0.1, "end": 0.1},
    "search": {"product_view": 0.6, "search": 0.2, "home": 0.1, "end": 0.1},
    "product_view": {"add_to_cart": 0.3, "search": 0.3, "home": 0.1, "product_view": 0.2, "end": 0.1},
    "add_to_cart": {"checkout": 0.5, "product_view": 0.3, "end": 0.2},
    "checkout": {"confirmation": 0.8, "end": 0.2},
    "confirmation": {"home": 0.5, "end": 0.5}
}

def generate_data():
    users = [f"user_{i:03d}" for i in range(NUM_USERS)]
    data = []
    
    print(f"Generating {TOTAL_EVENTS} events...")
    
    # Start time reference
    base_time = datetime.now() - timedelta(days=1)
    
    current_events = 0
    
    while current_events < TOTAL_EVENTS:
        user = random.choice(users)
        
        # Start a session
        current_page = "home"
        session_time = base_time + timedelta(minutes=random.randint(0, 24*60))
        
        while current_page != "end" and current_events < TOTAL_EVENTS:
            # Create event
            event = {
                "user_id": user,
                "event_time": session_time.strftime("%Y-%m-%d %H:%M:%S"),
                "current_page": current_page,
                "url": f"https://myshop.com/{current_page}",
                "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
                "device": random.choice(["Mobile", "Desktop", "Tablet"])
            }
            data.append(event)
            current_events += 1
            
            # Move to next page
            probs = TRANSITIONS.get(current_page, {"end": 1.0})
            next_page = random.choices(list(probs.keys()), weights=list(probs.values()))[0]
            
            # Time increment (random seconds between clicks)
            session_time += timedelta(seconds=random.randint(5, 120))
            
            current_page = next_page
            
            # Introduce occasional long breaks to simulate new sessions (30+ mins)
            if random.random() < 0.05: # 5% chance of a break
                 session_time += timedelta(minutes=random.randint(31, 120))

    # Sort by time to make it realistic log data
    data.sort(key=lambda x: x["event_time"])
    
    with open(OUTPUT_FILE, "w") as f:
        for entry in data:
            f.write(json.dumps(entry) + "\n")
            
    print(f"Successfully generated {len(data)} events in {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_data()
