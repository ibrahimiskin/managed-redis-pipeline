import redis
import logging


logger = logging.getLogger()


class Pipeline(redis.Redis):
    def __init__(self, host, port, db=0, batch_size=1000, dry_run=False):
        self.pipeline = redis.StrictRedis(host=host, port=port, db=db).pipeline()
        self.total = 0
        self.counter = 0
        self.batch_size = batch_size
        self.dry_run = dry_run

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.managed_close()

    def managed_execute(self):
        logger.debug("Pipeline will be executed")
        if not self.dry_run:
            response = self.pipeline.execute()
            if sum(response) != self.batch_size:
                logger.debug("[" + str(response) + "]")
                exit(1)
        self.counter = 0

    def managed_set(self, name, value, **kwargs):
        self.counter += 1
        self.total += 1
        if self.counter < self.batch_size:
            logger.debug("Set operation - key: {}, value: {}".format(name, value))
            if not self.dry_run:
                self.pipeline.set(name, value, **kwargs)
        else:
            self.pipeline.set(name, value, **kwargs)
            self.managed_execute()

    def managed_setex(self, name, time, value):
        self.counter += 1
        self.total += 1
        if self.counter < self.batch_size:
            logger.debug("Setex operation - key: {}, value: {}".format(name, value))
            if not self.dry_run:
                self.pipeline.setex(name, time, value)
        else:
            self.pipeline.setex(name, time, value)
            self.managed_execute()

    def managed_hmset(self, name, mapping):
        self.counter += 1
        self.total += 1
        if self.counter < self.batch_size:
            logger.debug("Hmset operation - key: {}, value: {}".format(name, mapping))
            if not self.dry_run:
                self.pipeline.hmset(name, mapping)
        else:
            self.pipeline.hmset(name, mapping)
            self.managed_execute()

    def managed_delete(self, *args):
        self.counter += 1
        self.total += 1
        if self.counter < self.batch_size:
            logger.debug("Delete operation - key: {}".format(args))
            if not self.dry_run:
                self.pipeline.delete(*args)
        else:
            self.pipeline.delete(*args)
            self.managed_execute()

    def managed_close(self):
        logger.debug("Pipeline will be executed at close")
        logger.debug("Executed total of {} commands".format(self.total))
        if not self.dry_run:
            self.pipeline.execute()
