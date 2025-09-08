import json
import random
import uuid
from datetime import datetime, timedelta
import time
from typing import Dict, List, Any
from dataclasses import dataclass, asdict
import faker

# Initialize faker for realistic data
fake = faker.Faker()

@dataclass
class Product:
    product_id: str
    name: str
    category: str
    price: float
    brand: str

@dataclass
class User:
    user_id: str
    email: str
    age: int
    location: str
    is_premium: bool

class EcommerceEventGenerator:
    def __init__(self):
        self.products = self._create_product_catalog()
        self.users = self._create_user_base()
        self.active_sessions = {}
        self.user_carts = {}  # Track what's in each user's cart
        
    def _create_product_catalog(self) -> List[Product]:
        """Create a realistic product catalog"""
        categories = {
            'electronics': ['laptop', 'smartphone', 'tablet', 'headphones', 'smartwatch'],
            'clothing': ['shirt', 'jeans', 'dress', 'shoes', 'jacket'],
            'home': ['lamp', 'chair', 'table', 'pillow', 'curtains'],
            'books': ['novel', 'cookbook', 'textbook', 'biography', 'manga'],
            'sports': ['yoga_mat', 'dumbbells', 'running_shoes', 'basketball', 'tennis_racket']
        }
        
        price_ranges = {
            'electronics': (50, 2000),
            'clothing': (15, 200),
            'home': (20, 500),
            'books': (10, 50),
            'sports': (25, 300)
        }
        
        brands = {
            'electronics': ['Apple', 'Samsung', 'Sony', 'Dell', 'HP'],
            'clothing': ['Nike', 'Adidas', 'H&M', 'Zara', 'Uniqlo'],
            'home': ['IKEA', 'West Elm', 'Pottery Barn', 'Wayfair', 'Target'],
            'books': ['Penguin', 'Random House', 'Scholastic', 'Harper', 'Simon'],
            'sports': ['Nike', 'Adidas', 'Under Armour', 'Wilson', 'Spalding']
        }
        
        products = []
        for category, items in categories.items():
            for item in items:
                for i in range(5):  # 5 variations per item
                    product_id = f"prod_{category}_{item}_{i+1:03d}"
                    name = f"{random.choice(brands[category])} {item.replace('_', ' ').title()}"
                    price = round(random.uniform(*price_ranges[category]), 2)
                    brand = random.choice(brands[category])
                    
                    products.append(Product(product_id, name, category, price, brand))
        
        return products
    
    def _create_user_base(self) -> List[User]:
        """Create a base of realistic users"""
        users = []
        for i in range(1000):  # 1000 users
            user_id = f"user_{i+1:06d}"
            email = fake.email()
            age = random.randint(18, 70)
            location = fake.city()
            is_premium = random.random() < 0.15  # 15% premium users
            
            users.append(User(user_id, email, age, location, is_premium))
        
        return users
    
    def _generate_base_event(self) -> Dict[str, Any]:
        """Generate base event structure"""
        return {
            'event_id': str(uuid.uuid4()),
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'ip_address': fake.ipv4(),
            'user_agent': fake.user_agent()
        }
    
    def generate_session_start(self) -> Dict[str, Any]:
        """Generate user session start event"""
        user = random.choice(self.users)
        session_id = str(uuid.uuid4())
        
        # Store active session
        self.active_sessions[user.user_id] = session_id
        if user.user_id not in self.user_carts:
            self.user_carts[user.user_id] = []
        
        event = self._generate_base_event()
        event.update({
            'event_type': 'session_start',
            'user_id': user.user_id,
            'session_id': session_id,
            'user_email': user.email,
            'user_age': user.age,
            'user_location': user.location,
            'is_premium_user': user.is_premium,
            'referrer': random.choice([
                'https://google.com',
                'https://facebook.com', 
                'https://instagram.com',
                'direct',
                'https://twitter.com'
            ])
        })
        
        return event
    
    def generate_page_view(self) -> Dict[str, Any]:
        """Generate page view event"""
        if not self.active_sessions:
            return self.generate_session_start()
        
        user_id = random.choice(list(self.active_sessions.keys()))
        session_id = self.active_sessions[user_id]
        product = random.choice(self.products)
        
        event = self._generate_base_event()
        event.update({
            'event_type': 'page_view',
            'user_id': user_id,
            'session_id': session_id,
            'product_id': product.product_id,
            'product_name': product.name,
            'product_category': product.category,
            'product_price': product.price,
            'product_brand': product.brand,
            'page_url': f'/products/{product.category}/{product.product_id}',
            'time_on_page': random.randint(5, 300)  # seconds
        })
        
        return event
    
    def generate_add_to_cart(self) -> Dict[str, Any]:
        """Generate add to cart event"""
        if not self.active_sessions:
            return self.generate_session_start()
        
        user_id = random.choice(list(self.active_sessions.keys()))
        session_id = self.active_sessions[user_id]
        product = random.choice(self.products)
        quantity = random.randint(1, 5)
        
        # Add to user's cart
        self.user_carts[user_id].append({
            'product_id': product.product_id,
            'quantity': quantity,
            'price': product.price
        })
        
        event = self._generate_base_event()
        event.update({
            'event_type': 'add_to_cart',
            'user_id': user_id,
            'session_id': session_id,
            'product_id': product.product_id,
            'product_name': product.name,
            'product_category': product.category,
            'product_price': product.price,
            'product_brand': product.brand,
            'quantity': quantity,
            'cart_total_items': len(self.user_carts[user_id])
        })
        
        return event
    
    def generate_remove_from_cart(self) -> Dict[str, Any]:
        """Generate remove from cart event"""
        # Find users with items in cart
        users_with_cart = [uid for uid, cart in self.user_carts.items() 
                          if cart and uid in self.active_sessions]
        
        if not users_with_cart:
            return self.generate_add_to_cart()
        
        user_id = random.choice(users_with_cart)
        session_id = self.active_sessions[user_id]
        
        # Remove random item from cart
        removed_item = self.user_carts[user_id].pop(random.randint(0, len(self.user_carts[user_id])-1))
        
        event = self._generate_base_event()
        event.update({
            'event_type': 'remove_from_cart',
            'user_id': user_id,
            'session_id': session_id,
            'product_id': removed_item['product_id'],
            'quantity': removed_item['quantity'],
            'cart_total_items': len(self.user_carts[user_id])
        })
        
        return event
    
    def generate_purchase(self) -> Dict[str, Any]:
        """Generate purchase event"""
        # Find users with items in cart
        users_with_cart = [uid for uid, cart in self.user_carts.items() 
                          if cart and uid in self.active_sessions]
        
        if not users_with_cart:
            # Create a quick add to cart then purchase
            self.generate_add_to_cart()
            return self.generate_purchase()
        
        user_id = random.choice(users_with_cart)
        session_id = self.active_sessions[user_id]
        cart = self.user_carts[user_id]
        
        # Calculate order totals
        subtotal = sum(item['price'] * item['quantity'] for item in cart)
        tax = round(subtotal * 0.08, 2)  # 8% tax
        shipping = 0 if subtotal > 50 else 9.99  # Free shipping over $50
        total = round(subtotal + tax + shipping, 2)
        
        event = self._generate_base_event()
        event.update({
            'event_type': 'purchase',
            'user_id': user_id,
            'session_id': session_id,
            'order_id': f"order_{uuid.uuid4().hex[:8]}",
            'items': cart,
            'total_items': sum(item['quantity'] for item in cart),
            'subtotal': subtotal,
            'tax': tax,
            'shipping': shipping,
            'total': total,
            'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'apple_pay']),
            'coupon_used': random.random() < 0.3,  # 30% use coupons
            'discount_amount': round(random.uniform(5, 50), 2) if random.random() < 0.3 else 0
        })
        
        # Clear cart after purchase
        self.user_carts[user_id] = []
        
        return event
    
    def generate_session_end(self) -> Dict[str, Any]:
        """Generate session end event"""
        if not self.active_sessions:
            return self.generate_session_start()
        
        user_id = random.choice(list(self.active_sessions.keys()))
        session_id = self.active_sessions[user_id]
        
        # Remove from active sessions
        del self.active_sessions[user_id]
        
        # Calculate session duration
        session_duration = random.randint(60, 3600)  # 1 minute to 1 hour
        
        event = self._generate_base_event()
        event.update({
            'event_type': 'session_end',
            'user_id': user_id,
            'session_id': session_id,
            'session_duration_seconds': session_duration,
            'pages_viewed': random.randint(1, 20),
            'items_added_to_cart': random.randint(0, 8),
            'purchased': random.random() < 0.05  # 5% conversion rate
        })
        
        return event
    
    def generate_random_event(self) -> Dict[str, Any]:
        """Generate a random event with realistic probabilities"""
        event_types = [
            ('session_start', 0.10),
            ('page_view', 0.40),
            ('add_to_cart', 0.20),
            ('remove_from_cart', 0.05),
            ('purchase', 0.15),
            ('session_end', 0.10)
        ]
        
        # Choose event type based on probabilities
        rand = random.random()
        cumulative = 0
        
        for event_type, probability in event_types:
            cumulative += probability
            if rand <= cumulative:
                return getattr(self, f'generate_{event_type}')()
        
        return self.generate_page_view()  # fallback

def main():
    """Main function to run the event generator"""
    generator = EcommerceEventGenerator()
    
    print("🚀 E-commerce Event Generator Started!")
    print("Generating events... (Press Ctrl+C to stop)")
    
    try:
        event_count = 0
        while True:
            event = generator.generate_random_event()
            
            # Output as JSON (this will later go to Kafka)
            print(json.dumps(event, indent=2))
            print("-" * 50)
            
            event_count += 1
            if event_count % 10 == 0:
                print(f"📊 Generated {event_count} events so far...")
            
            # Wait between events (simulate real-time)
            time.sleep(random.uniform(0.1, 2.0))  # 0.1 to 2 seconds
            
    except KeyboardInterrupt:
        print(f"\n✅ Event generation stopped. Total events generated: {event_count}")

if __name__ == "__main__":
    main()