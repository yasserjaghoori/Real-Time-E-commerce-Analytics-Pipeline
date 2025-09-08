import json
import pandas as pd
from kafka import KafkaConsumer
from collections import defaultdict, Counter
import time
from datetime import datetime, timedelta
import threading
import signal
import sys

class EcommerceAnalytics:
    def __init__(self):
        self.events = []
        self.analytics_data = {
            'pageviews': [],
            'purchases': [],
            'cart_events': [],
            'sessions': []
        }
        self.running = True
        
    def consume_events(self, topic, consumer_group):
        """Consume events from a specific Kafka topic"""
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092'],
            group_id=consumer_group,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        print(f"📡 Started consumer for topic: {topic}")
        
        for message in consumer:
            if not self.running:
                break
                
            event = message.value
            event['kafka_timestamp'] = datetime.now()
            
            # Store event in appropriate category
            if topic == 'ecommerce-pageviews':
                self.analytics_data['pageviews'].append(event)
            elif topic == 'ecommerce-purchases':
                self.analytics_data['purchases'].append(event)
            elif topic == 'ecommerce-cart-events':
                self.analytics_data['cart_events'].append(event)
            elif topic == 'ecommerce-sessions':
                self.analytics_data['sessions'].append(event)
                
            self.events.append(event)
            
        consumer.close()
    
    def analyze_popular_products(self, time_window_minutes=5):
        """Analyze popular products in the last time window"""
        cutoff_time = datetime.now() - timedelta(minutes=time_window_minutes)
        
        recent_pageviews = [
            event for event in self.analytics_data['pageviews']
            if event.get('kafka_timestamp', datetime.now()) > cutoff_time
        ]
        
        if not recent_pageviews:
            return pd.DataFrame()
        
        # Count views by product
        product_views = Counter()
        category_views = Counter()
        
        for event in recent_pageviews:
            product_name = event.get('product_name', 'Unknown')
            category = event.get('product_category', 'Unknown')
            
            product_views[product_name] += 1
            category_views[category] += 1
        
        # Create DataFrame
        products_df = pd.DataFrame([
            {
                'product': product,
                'views': count,
                'category': next((event.get('product_category', 'Unknown') 
                                for event in recent_pageviews 
                                if event.get('product_name') == product), 'Unknown')
            }
            for product, count in product_views.most_common(10)
        ])
        
        return products_df
    
    def analyze_revenue(self, time_window_minutes=5):
        """Analyze revenue metrics in the last time window"""
        cutoff_time = datetime.now() - timedelta(minutes=time_window_minutes)
        
        recent_purchases = [
            event for event in self.analytics_data['purchases']
            if event.get('kafka_timestamp', datetime.now()) > cutoff_time
        ]
        
        if not recent_purchases:
            return {}
        
        total_revenue = sum(event.get('total', 0) for event in recent_purchases)
        total_orders = len(recent_purchases)
        avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
        total_items = sum(event.get('total_items', 0) for event in recent_purchases)
        unique_customers = len(set(event.get('user_id') for event in recent_purchases))
        
        return {
            'total_revenue': round(total_revenue, 2),
            'total_orders': total_orders,
            'avg_order_value': round(avg_order_value, 2),
            'total_items_sold': total_items,
            'unique_customers': unique_customers
        }
    
    def analyze_conversion_rate(self, time_window_minutes=5):
        """Calculate conversion rate in the last time window"""
        cutoff_time = datetime.now() - timedelta(minutes=time_window_minutes)
        
        recent_pageviews = [
            event for event in self.analytics_data['pageviews']
            if event.get('kafka_timestamp', datetime.now()) > cutoff_time
        ]
        
        recent_purchases = [
            event for event in self.analytics_data['purchases']
            if event.get('kafka_timestamp', datetime.now()) > cutoff_time
        ]
        
        unique_viewers = len(set(event.get('user_id') for event in recent_pageviews))
        unique_buyers = len(set(event.get('user_id') for event in recent_purchases))
        
        conversion_rate = (unique_buyers / unique_viewers * 100) if unique_viewers > 0 else 0
        
        return {
            'unique_viewers': unique_viewers,
            'unique_buyers': unique_buyers,
            'conversion_rate_percent': round(conversion_rate, 2)
        }
    
    def print_analytics_dashboard(self):
        """Print a real-time analytics dashboard"""
        while self.running:
            try:
                # Clear screen (works on most terminals)
                print("\033[2J\033[H", end="")
                
                print("=" * 80)
                print("🎯 REAL-TIME E-COMMERCE ANALYTICS DASHBOARD")
                print("=" * 80)
                print(f"⏰ Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"📊 Total Events Processed: {len(self.events)}")
                print()
                
                # Popular Products
                print("🔥 TOP PRODUCTS (Last 5 minutes)")
                print("-" * 50)
                popular_products = self.analyze_popular_products()
                if not popular_products.empty:
                    for _, row in popular_products.head(5).iterrows():
                        print(f"   {row['views']:3d} views - {row['product']} ({row['category']})")
                else:
                    print("   No page views in the last 5 minutes")
                print()
                
                # Revenue Analytics
                print("💰 REVENUE METRICS (Last 5 minutes)")
                print("-" * 50)
                revenue_metrics = self.analyze_revenue()
                if revenue_metrics:
                    print(f"   Total Revenue: ${revenue_metrics['total_revenue']}")
                    print(f"   Total Orders: {revenue_metrics['total_orders']}")
                    print(f"   Avg Order Value: ${revenue_metrics['avg_order_value']}")
                    print(f"   Items Sold: {revenue_metrics['total_items_sold']}")
                    print(f"   Unique Customers: {revenue_metrics['unique_customers']}")
                else:
                    print("   No purchases in the last 5 minutes")
                print()
                
                # Conversion Rate
                print("📈 CONVERSION METRICS (Last 5 minutes)")
                print("-" * 50)
                conversion_metrics = self.analyze_conversion_rate()
                print(f"   Unique Viewers: {conversion_metrics['unique_viewers']}")
                print(f"   Unique Buyers: {conversion_metrics['unique_buyers']}")
                print(f"   Conversion Rate: {conversion_metrics['conversion_rate_percent']}%")
                print()
                
                # Event Breakdown
                print("📋 EVENT BREAKDOWN")
                print("-" * 50)
                print(f"   Page Views: {len(self.analytics_data['pageviews'])}")
                print(f"   Purchases: {len(self.analytics_data['purchases'])}")
                print(f"   Cart Events: {len(self.analytics_data['cart_events'])}")
                print(f"   Sessions: {len(self.analytics_data['sessions'])}")
                print()
                
                print("💡 Tip: Start your Kafka producer to see live data!")
                print("🛑 Press Ctrl+C to stop")
                
                time.sleep(10)  # Update every 10 seconds
                
            except Exception as e:
                print(f"❌ Error in dashboard: {e}")
                time.sleep(5)

def signal_handler(sig, frame, analytics):
    """Handle Ctrl+C gracefully"""
    print("\n🛑 Shutting down analytics...")
    analytics.running = False
    sys.exit(0)

def main():
    print("🚀 Starting E-commerce Real-Time Analytics...")
    print("📡 Connecting to Kafka...")
    
    analytics = EcommerceAnalytics()
    
    # Set up signal handler for graceful shutdown
    signal.signal(signal.SIGINT, lambda sig, frame: signal_handler(sig, frame, analytics))
    
    # Start consumer threads for different topics
    topics = [
        'ecommerce-pageviews',
        'ecommerce-purchases', 
        'ecommerce-cart-events',
        'ecommerce-sessions'
    ]
    
    threads = []
    for topic in topics:
        thread = threading.Thread(
            target=analytics.consume_events,
            args=(topic, f"analytics-group-{topic}"),
            daemon=True
        )
        thread.start()
        threads.append(thread)
    
    print("✅ Kafka consumers started!")
    print("📊 Starting analytics dashboard...")
    time.sleep(2)
    
    # Start analytics dashboard
    try:
        analytics.print_analytics_dashboard()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
        analytics.running = False

if __name__ == "__main__":
    main()