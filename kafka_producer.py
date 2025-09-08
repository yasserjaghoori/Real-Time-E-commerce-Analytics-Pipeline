import json
import time
import random
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
from event_generator import EcommerceEventGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaEventProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Initialize Kafka producer"""
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.event_generator = EcommerceEventGenerator()
        
        # Topic configuration
        self.topics = {
            'session_start': 'ecommerce-sessions',
            'session_end': 'ecommerce-sessions',
            'page_view': 'ecommerce-pageviews',
            'add_to_cart': 'ecommerce-cart-events',
            'remove_from_cart': 'ecommerce-cart-events',
            'purchase': 'ecommerce-purchases'
        }
        
        self.connect_to_kafka()
    
    def connect_to_kafka(self):
        """Connect to Kafka cluster"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,
                batch_size=16384,
                linger_ms=10,
                buffer_memory=33554432,
                compression_type='gzip'
            )
            logger.info("✅ Successfully connected to Kafka!")
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            raise
    
    def send_event(self, event):
        """Send a single event to appropriate Kafka topic"""
        try:
            event_type = event['event_type']
            topic = self.topics.get(event_type, 'ecommerce-general')
            
            # Use user_id as key for partitioning (events from same user go to same partition)
            key = event.get('user_id', str(random.randint(1, 100)))
            
            # Send to Kafka
            future = self.producer.send(
                topic=topic,
                key=key,
                value=event,
                timestamp_ms=int(time.time() * 1000)
            )
            
            # Add callback for success/error handling
            future.add_callback(self.on_send_success)
            future.add_errback(self.on_send_error)
            
            return future
            
        except Exception as e:
            logger.error(f"❌ Error sending event: {e}")
            return None
    
    def on_send_success(self, record_metadata):
        """Callback for successful sends"""
        logger.debug(f"✅ Event sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
    
    def on_send_error(self, excp):
        """Callback for failed sends"""
        logger.error(f"❌ Failed to send event: {excp}")
    
    def produce_events(self, num_events=None, delay_range=(0.1, 2.0)):
        """Produce events continuously or for specified count"""
        logger.info("🚀 Starting event production...")
        
        event_count = 0
        events_per_topic = {}
        
        try:
            while num_events is None or event_count < num_events:
                # Generate event
                event = self.event_generator.generate_random_event()
                
                # Send to Kafka
                future = self.send_event(event)
                
                if future:
                    event_count += 1
                    event_type = event['event_type']
                    events_per_topic[event_type] = events_per_topic.get(event_type, 0) + 1
                    
                    # Log progress
                    if event_count % 10 == 0:
                        logger.info(f"📊 Sent {event_count} events to Kafka")
                    
                    # Log individual events (can be disabled for high volume)
                    if event_count <= 20:  # Only log first 20 for debugging
                        logger.info(f"📤 Sent {event_type} event to topic {self.topics.get(event_type)}")
                    
                    # Flush producer buffer periodically
                    if event_count % 100 == 0:
                        self.producer.flush(timeout=10)
                
                # Random delay between events
                if delay_range:
                    time.sleep(random.uniform(*delay_range))
                    
        except KeyboardInterrupt:
            logger.info("\n🛑 Stopping event production...")
            
        finally:
            # Final flush and close
            if self.producer:
                self.producer.flush(timeout=30)
                self.producer.close()
            
            # Final stats
            logger.info(f"✅ Production complete! Total events: {event_count}")
            logger.info("📈 Events per topic:")
            for topic, count in events_per_topic.items():
                logger.info(f"   {topic}: {count}")

def main():
    """Main function"""
    print("🎯 Kafka E-commerce Event Producer")
    print("=" * 50)
    
    # Wait for user confirmation that Kafka is running
    input("⚠️  Make sure Kafka is running with 'docker-compose up -d' first, then press Enter...")
    
    try:
        # Create producer
        producer = KafkaEventProducer()
        
        # Ask user for production mode
        print("\nProduction options:")
        print("1. Continuous production (Ctrl+C to stop)")
        print("2. Produce specific number of events")
        
        choice = input("Choose option (1 or 2): ").strip()
        
        if choice == "2":
            num_events = int(input("How many events to produce? "))
            producer.produce_events(num_events=num_events)
        else:
            producer.produce_events()
            
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        logger.error(f"💥 Error: {e}")

if __name__ == "__main__":
    main()