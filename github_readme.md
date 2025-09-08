# Real-Time E-commerce Analytics Pipeline

A comprehensive end-to-end data pipeline that simulates and analyzes e-commerce events in real-time using Apache Kafka, Docker, and Python.

## Architecture Overview

```
Event Generator → Kafka Producer → Kafka Topics → Analytics Consumer → Dashboard
```

This project demonstrates modern data engineering practices including:
- Event-driven architecture
- Real-time stream processing
- Containerized infrastructure
- Time-windowed analytics

## Features

- **Realistic Event Generation**: Simulates 1,000 users and 125 products with stateful shopping behavior
- **Real-Time Streaming**: Apache Kafka processes events across 4 specialized topics
- **Live Analytics Dashboard**: Calculates conversion rates, revenue metrics, and product popularity
- **Containerized Deployment**: Docker Compose orchestrates the entire infrastructure
- **Time-Windowed Analysis**: 5-minute rolling windows for real-time business insights

## Technology Stack

- **Streaming**: Apache Kafka, Zookeeper
- **Analytics**: Python, Pandas
- **Infrastructure**: Docker, Docker Compose
- **Monitoring**: Kafka UI for real-time topic monitoring

## Project Structure

```
├── docker-compose.yml          # Infrastructure orchestration
├── event_generator.py          # E-commerce event simulation
├── kafka_producer.py          # Streams events to Kafka topics
├── kafka_analytics.py         # Real-time analytics consumer
├── spark-apps/                # Spark application directory
└── spark-data/                # Spark data directory
```

## Quick Start

### Prerequisites
- Docker Desktop
- Python 3.8+
- Required Python packages: `kafka-python`, `faker`, `pandas`

### Installation

1. Clone the repository:
```bash
git clone https://github.com/[your-username]/real-time-ecommerce-analytics-pipeline.git
cd real-time-ecommerce-analytics-pipeline
```

2. Install Python dependencies:
```bash
pip install kafka-python faker pandas
```

3. Start the Kafka infrastructure:
```bash
docker-compose up -d
```

4. Verify services are running:
```bash
docker-compose ps
```

### Running the Pipeline

1. **Start the analytics consumer** (in one terminal):
```bash
python kafka_analytics.py
```

2. **Start the event producer** (in another terminal):
```bash
python kafka_producer.py
```

3. **Monitor Kafka topics** via the web UI:
   - Open http://localhost:8080 in your browser
   - View real-time message flow across topics

## Analytics Dashboard

The live dashboard displays:

- **Popular Products**: Top-viewed items in 5-minute windows
- **Revenue Metrics**: Total sales, average order value, unique customers
- **Conversion Analytics**: Viewer-to-buyer conversion rates
- **Event Breakdown**: Real-time counts by event type

## Key Kafka Topics

- `ecommerce-pageviews`: Product page visits and user interactions
- `ecommerce-cart-events`: Add/remove cart actions
- `ecommerce-purchases`: Order completions and revenue data
- `ecommerce-sessions`: User session start/end events

## Event Schema Examples

**Page View Event:**
```json
{
  "event_id": "uuid-string",
  "event_type": "page_view",
  "user_id": "user_000123",
  "product_id": "prod_electronics_laptop_001",
  "product_category": "electronics",
  "product_price": 999.99,
  "timestamp": "2025-09-07T15:30:00Z"
}
```

**Purchase Event:**
```json
{
  "event_id": "uuid-string",
  "event_type": "purchase",
  "user_id": "user_000456",
  "order_id": "order_abc123",
  "total": 1299.97,
  "items": [...],
  "timestamp": "2025-09-07T15:35:00Z"
}
```

## Performance Characteristics

- **Event Generation**: 1-2 events per second with realistic user behavior
- **Processing Latency**: Sub-second event processing
- **Analytics Refresh**: 10-second dashboard updates
- **Data Retention**: Configurable topic retention policies

## Business Metrics Calculated

- **Conversion Rate**: Percentage of viewers who make purchases
- **Average Order Value**: Mean transaction value
- **Product Popularity**: View counts by category and product
- **Customer Engagement**: Session duration and page views per session

## Future Enhancements

- [ ] Data persistence layer (PostgreSQL/MongoDB)
- [ ] Advanced stream processing with Apache Spark
- [ ] Web-based dashboard with real-time charts
- [ ] Machine learning for customer behavior prediction
- [ ] Alert system for anomaly detection

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## Contact

**Yasser Jaghoori**  
Data Analytics Engineering Student  
Email: jaghooya@outlook.com  
LinkedIn: [Your LinkedIn Profile]
