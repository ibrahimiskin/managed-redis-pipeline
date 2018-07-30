# managed-redis-pipeline
This library wraps redis-py's pipeline and executes commands at specified batch size

```python
import redis
from managed_pipeline import Pipeline

pool = redis.ConnectionPool(host="localhost", port=6379, db=0)


with Pipeline(pool, batch_size=2) as pipeline:
    pipeline.hmset("name", {"key1": "value1"})
    pipeline.delete("name")
    
    pipeline.managed_set("managed_set", "value")
    pipeline.managed_setex("managed_setex", "value", 33333)
    pipeline.managed_hmset("managed_hmset", {"key1": "value1"})
    pipeline.managed_delete("key")
```