-- Fix Kafka dashboard template:
-- 1. Consumer Group variable uses kafka.consumer_group.lag which doesn't exist
--    (Redpanda doesn't emit a pre-calculated lag metric). Switch to
--    kafka.consumer_group.offset which exists.
-- 2. Replace kafka.consumer_group.lag widgets with PromQL-calculated lag
--    expression: partition_current_offset - consumer_group_offset.

UPDATE dashboard_templates
SET template_config = '{
    "variables": [
        {"name": "topic", "label": "Topic", "type": "query", "query": "label_values(kafka.topic.partitions, kafka.topic)"},
        {"name": "consumer_group", "label": "Consumer Group", "type": "query", "query": "label_values(kafka.consumer_group.offset, kafka.consumer_group)"}
    ],
    "tabs": [
        {
            "name": "Overview",
            "icon": "send",
            "widgets": [
                {"type": "stat", "title": "Active Brokers", "x": 0, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "kafka.brokers", "instant": true}}},
                {"type": "stat", "title": "Total Partitions", "x": 3, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(kafka.topic.partitions{kafka.topic=~\"$topic\"})", "instant": true}}},
                {"type": "stat", "title": "Total Consumer Lag", "x": 6, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "sum(kafka.partition.current_offset{kafka.topic=~\"$topic\"}) - sum(kafka.consumer_group.offset{kafka.consumer_group=~\"$consumer_group\"})", "instant": true}, "thresholds": [{"value": 0, "color": "green"}, {"value": 1000, "color": "orange"}, {"value": 10000, "color": "red"}]}},
                {"type": "stat", "title": "Consumer Groups", "x": 9, "y": 0, "w": 3, "h": 2, "config": {"query": {"promql": "count(count by (kafka.consumer_group) (kafka.consumer_group.offset{kafka.consumer_group=~\"$consumer_group\"}))", "instant": true}}},
                {"type": "timeseries", "title": "Partition Offset Rate (approx msgs/s)", "x": 0, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (kafka.topic) (rate(kafka.partition.current_offset{kafka.topic=~\"$topic\"}[5m]))"}, "unit": "ops"}},
                {"type": "timeseries", "title": "Consumer Group Lag", "x": 6, "y": 2, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (kafka.consumer_group) (kafka.partition.current_offset - on(kafka.topic, kafka.partition) group_right(kafka.consumer_group) kafka.consumer_group.offset{kafka.consumer_group=~\"$consumer_group\"})"}}},
                {"type": "timeseries", "title": "Under-Replicated Partitions", "x": 0, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (kafka.topic) (kafka.partition.replicas{kafka.topic=~\"$topic\"} - kafka.partition.replicas_in_sync{kafka.topic=~\"$topic\"})"}, "thresholds": [{"value": 0, "color": "green"}, {"value": 1, "color": "red"}]}},
                {"type": "timeseries", "title": "Consumer Group Offset Rate", "x": 6, "y": 6, "w": 6, "h": 4, "config": {"query": {"promql": "sum by (kafka.consumer_group) (rate(kafka.consumer_group.offset{kafka.consumer_group=~\"$consumer_group\"}[5m]))"}, "unit": "ops"}},
                {"type": "timeseries", "title": "Partition Count by Topic", "x": 0, "y": 10, "w": 6, "h": 4, "config": {"query": {"promql": "kafka.topic.partitions{kafka.topic=~\"$topic\"}"}}},
                {"type": "timeseries", "title": "Consumer Group Members", "x": 6, "y": 10, "w": 6, "h": 4, "config": {"query": {"promql": "kafka.consumer_group.members{kafka.consumer_group=~\"$consumer_group\"}"}}}
            ]
        }
    ]
}'::jsonb
WHERE name = 'Kafka';
