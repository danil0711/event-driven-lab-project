def get_retry_topic(retry_count: int, settings):
    if retry_count == 1:
        return settings.kafka_inventory_retry_1s_topic
    elif retry_count == 2:
        return settings.kafka_inventory_retry_10s_topic
    elif retry_count == 3:
        return settings.kafka_inventory_retry_1m_topic
    else:
        return settings.kafka_inventory_dlq_topic