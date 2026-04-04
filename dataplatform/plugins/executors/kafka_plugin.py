import json
import logging

logger = logging.getLogger(__name__)


class KafkaExecutor:
    """Apache Kafka executor for event streaming operations."""

    def execute(self, config: dict) -> tuple[bool, dict]:
        """
        Execute Kafka operations.
        
        config = {
            "operation": "publish" | "consume" | "create_topic",
            "brokers": ["localhost:9092"],
            "topic": "my_topic",
            "message": {"data": "value"},
            ...
        }
        """
        try:
            operation = config.get("operation", "publish")

            if operation == "publish":
                return self._publish_message(config)
            elif operation == "consume":
                return self._consume_messages(config)
            elif operation == "create_topic":
                return self._create_topic(config)
            else:
                return False, {"error": f"Unknown operation: {operation}"}

        except ImportError:
            logger.warning("kafka-python not installed. Install with: pip install kafka-python")
            return False, {"error": "kafka-python not installed"}
        except Exception as e:
            logger.error(f"Kafka error: {e}")
            return False, {"error": str(e)}

    def _publish_message(self, config: dict) -> tuple[bool, dict]:
        """Publish message to Kafka topic."""
        try:
            from kafka import KafkaProducer

            brokers = config.get("brokers", ["localhost:9092"])
            topic = config.get("topic")
            message = config.get("message")
            partition = config.get("partition")

            if not topic or message is None:
                return False, {"error": "topic and message required"}

            producer = KafkaProducer(
                bootstrap_servers=brokers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )

            if isinstance(message, list):
                # Batch publish
                for msg in message:
                    producer.send(topic, value=msg, partition=partition)
                count = len(message)
            else:
                # Single publish
                producer.send(topic, value=message, partition=partition)
                count = 1

            producer.flush()
            producer.close()

            logger.info(f"✓ Published {count} message(s) to {topic}")

            return True, {
                "topic": topic,
                "messages_published": count
            }

        except ImportError:
            logger.warning("kafka-python not installed")
            return False, {"error": "kafka-python not installed"}
        except Exception as e:
            logger.error(f"Failed to publish: {e}")
            return False, {"error": str(e)}

    def _consume_messages(self, config: dict) -> tuple[bool, dict]:
        """Consume messages from Kafka topic."""
        try:
            from kafka import KafkaConsumer

            brokers = config.get("brokers", ["localhost:9092"])
            topic = config.get("topic")
            max_messages = config.get("max_messages", 10)
            timeout_ms = config.get("timeout_ms", 5000)
            group_id = config.get("group_id", "default_group")

            if not topic:
                return False, {"error": "topic required"}

            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=brokers,
                group_id=group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                consumer_timeout_ms=timeout_ms
            )

            messages = []
            for idx, message in enumerate(consumer):
                if idx >= max_messages:
                    break
                messages.append({
                    "offset": message.offset,
                    "partition": message.partition,
                    "value": message.value
                })

            consumer.close()

            logger.info(f"✓ Consumed {len(messages)} message(s) from {topic}")

            return True, {
                "topic": topic,
                "messages_count": len(messages),
                "messages": messages
            }

        except ImportError:
            logger.warning("kafka-python not installed")
            return False, {"error": "kafka-python not installed"}
        except Exception as e:
            logger.error(f"Failed to consume: {e}")
            return False, {"error": str(e)}

    def _create_topic(self, config: dict) -> tuple[bool, dict]:
        """Create Kafka topic."""
        try:
            from kafka.admin import KafkaAdminClient, NewTopic

            brokers = config.get("brokers", ["localhost:9092"])
            topic = config.get("topic")
            partitions = config.get("partitions", 1)
            replication_factor = config.get("replication_factor", 1)

            if not topic:
                return False, {"error": "topic required"}

            admin_client = KafkaAdminClient(bootstrap_servers=brokers)

            topic_obj = NewTopic(
                name=topic,
                num_partitions=partitions,
                replication_factor=replication_factor
            )

            fs = admin_client.create_topics(new_topics=[topic_obj], validate_only=False)
            fs[topic].result()
            admin_client.close()

            logger.info(f"✓ Created Kafka topic: {topic}")

            return True, {
                "topic": topic,
                "partitions": partitions,
                "replication_factor": replication_factor
            }

        except ImportError:
            logger.warning("kafka-python not installed")
            return False, {"error": "kafka-python not installed"}
        except Exception as e:
            logger.error(f"Failed to create topic: {e}")
            return False, {"error": str(e)}
