# managed-redis-pipeline
This library wraps redis-py's pipeline and executes commands at specified batch size

```python
import redis
from managed_pipeline import Pipeline

pool = redis.ConnectionPool("localhost", port=6379, db=0)
redis = redis.Redis(connection_pool=pool)


with Pipeline(pool, batch_size=500) as pipeline:
    pipeline.hmset("key", {"key": "value"})
    pipeline.delete("key")
```