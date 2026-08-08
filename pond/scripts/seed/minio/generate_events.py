#!/usr/bin/env python3
"""
Generate test events Parquet file for MinIO/S3 federated query testing.
Events reference order IDs from the MySQL orders table.
"""

import os
import json
from datetime import datetime, timedelta

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("Installing required packages...")
    import subprocess
    subprocess.check_call(['pip3', 'install', 'pyarrow'])
    import pyarrow as pa
    import pyarrow.parquet as pq


def generate_events(num_events: int = 5000) -> pa.Table:
    """Generate deterministic event data that references MySQL orders."""
    
    event_types = [
        'order_created',
        'payment_received', 
        'payment_failed',
        'order_confirmed',
        'shipment_created',
        'shipment_dispatched',
        'delivery_attempted',
        'delivery_completed',
        'order_cancelled',
        'refund_initiated'
    ]
    
    source_systems = ['web_app', 'mobile_app', 'api', 'batch_job', 'admin_panel']
    
    event_ids = []
    order_ids = []
    types = []
    event_data_list = []
    source_systems_list = []
    timestamps = []
    customer_ids = []
    
    base_time = datetime(2024, 1, 1, 0, 0, 0)
    
    for i in range(1, num_events + 1):
        # Event ID
        event_ids.append(f"EVT-{i:08d}")
        
        # Order ID (1-1000, matching MySQL orders)
        order_id = ((i - 1) % 1000) + 1
        order_ids.append(order_id)
        
        # Customer ID (1-100, derived from order for consistency)
        customer_id = ((order_id - 1) % 100) + 1
        customer_ids.append(customer_id)
        
        # Event type
        event_type = event_types[(i - 1) % len(event_types)]
        types.append(event_type)
        
        # Event data as JSON
        event_data = {
            "order_number": f"ORD-{order_id:06d}",
            "sequence": (i - 1) // 1000 + 1,
            "metadata": {
                "ip_address": f"192.168.{(i % 255)}.{((i * 7) % 255)}",
                "user_agent": f"Mozilla/5.0 (Test Agent {i % 10})",
                "session_id": f"sess_{i:010d}"
            }
        }
        
        # Add type-specific data
        if event_type == 'payment_received':
            event_data["amount"] = round(10.0 + (i % 500) * 0.5, 2)
            event_data["payment_method"] = ['credit_card', 'debit_card', 'paypal', 'bank_transfer'][i % 4]
        elif event_type == 'shipment_dispatched':
            event_data["carrier"] = ['FedEx', 'UPS', 'USPS', 'DHL'][i % 4]
            event_data["tracking_number"] = f"TRK{i:012d}"
        elif event_type == 'delivery_completed':
            event_data["signature_required"] = i % 3 == 0
            event_data["delivery_notes"] = f"Delivered to {'front door' if i % 2 == 0 else 'mailroom'}"
            
        event_data_list.append(json.dumps(event_data))
        
        # Source system
        source_systems_list.append(source_systems[(i - 1) % len(source_systems)])
        
        # Timestamp (spread over ~42 days, roughly 5000/120 events per hour)
        timestamp = base_time + timedelta(minutes=i * 12)
        timestamps.append(timestamp)
    
    # Create PyArrow table
    table = pa.table({
        'event_id': pa.array(event_ids, type=pa.string()),
        'order_id': pa.array(order_ids, type=pa.int32()),
        'customer_id': pa.array(customer_ids, type=pa.int32()),
        'event_type': pa.array(types, type=pa.string()),
        'event_data': pa.array(event_data_list, type=pa.string()),
        'source_system': pa.array(source_systems_list, type=pa.string()),
        'timestamp': pa.array(timestamps, type=pa.timestamp('us'))
    })
    
    return table


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, 'events.parquet')
    
    print("Generating 5000 test events...")
    table = generate_events(5000)
    
    print(f"Writing to {output_path}...")
    pq.write_table(
        table, 
        output_path,
        compression='snappy',
        row_group_size=1000
    )
    
    # Print summary
    print(f"\nParquet file created: {output_path}")
    print(f"Total rows: {table.num_rows}")
    print(f"Schema:")
    for field in table.schema:
        print(f"  - {field.name}: {field.type}")
    
    # File size
    size_bytes = os.path.getsize(output_path)
    print(f"\nFile size: {size_bytes / 1024:.1f} KB")


if __name__ == '__main__':
    main()
