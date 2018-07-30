import redis
import logging


log = logging.getLogger()


class Pipeline(redis.Redis):
    def __init__(self, redis_pool, dry_run=False, batch_size=1000, *args):
        self.pipeline = redis.Redis(connection_pool=redis_pool).pipeline(*args)
        self.counter = 0
        self.batch_size = batch_size
        self.dry_run = dry_run

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.managed_close()

    def managed_hmset(self, name, mapping):
        if self.counter < self.batch_size:
            log.debug("Hmset operation - key: {}, value: {}".format(name, mapping))
            if not self.dry_run:
                self.pipeline.hmset(name, mapping)
            self.counter += 1
        else:
            log.debug("Pipeline will be executed")
            if not self.dry_run:
                self.pipeline.execute()
            self.counter = 0

    def managed_delete(self, *args):
        if self.counter < self.batch_size:
            log.debug("Delete operation - key: {}".format(args))
            if not self.dry_run:
                self.pipeline.delete(*args)
            self.counter += 1
        else:
            log.debug("Pipeline will be executed")
            if not self.dry_run:
                self.pipeline.execute()
            self.counter = 0

    def managed_close(self):
        if self.counter > 0:
            log.debug("Pipeline will be executed")
            if not self.dry_run:
                self.pipeline.execute()
