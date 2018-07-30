# managed-redis-pipeline
This library wraps redis-py's pipeline and executes commands at specified batch size

```python

from managed_pipeline import Pipeline


with Pipeline("localhost", 6379, batch_size=2) as pipeline:
    pipeline.managed_set("managed_set", "value")
    pipeline.managed_setex("managed_setex", "value", 33333)
    pipeline.managed_hmset("managed_hmset", {"key1": "value1"})
    pipeline.managed_delete("key")
```