import copyreg
import logging
import pickle

from hashlib import md5

from odoo.tools.lru import LRU
from odoo.tools.func import locked
from odoo.tools.misc import frozendict

_logger = logging.getLogger(__name__)


def _pickle_frozendict(fd):
    """
    Pickle's default deserialization creates an empty dict and adds items one-by-one.
    frozendict is immutable and doesn't support __setitem__, so this fails.
    This reducer tells pickle to call frozendict(dict_data) directly instead.

    Example:
        Step 1: Create EMPTY instance
            obj = frozendict.__new__(frozendict)  # obj = frozendict()
        Step 2: Add items ONE BY ONE
            obj['a'] = 1  ← ❌ NotImplementedError!
            obj['b'] = 2  ← Never reaches this
    """
    return frozendict, (dict(fd),)


copyreg.pickle(frozendict, _pickle_frozendict)


class RedisLRU(LRU):
    def __init__(self, count, pairs=(), **kwargs):
        super().__init__(count, pairs)

        self.dredis = kwargs.pop('redis_client', None)
        self.expiration = kwargs.pop('expiration', 3600)
        self.prefix = kwargs.pop('prefix', 'odoo_cache:')

    def _get_redis_key(self, key):
        new_key = list(key)
        for i, k in enumerate(new_key):
            if callable(k):
                new_key[i] = k.__name__

        return f"{self.prefix}{md5(str(tuple(new_key)).encode('utf-8')).hexdigest()}"

    @staticmethod
    def _encode_value(value):
        """
        Encode a value for Redis storage using pickle.
        Returns None if the value cannot be pickled (dynamic classes, circular refs, etc.)
        """
        try:
            return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as ex:
            _logger.debug("Cannot pickle value for Redis cache: \n%s", ex)
            return None

    @staticmethod
    def _decode_value(data):
        """
        Decode a value from Redis storage using pickle.
        Returns None if deserialization fails (triggers cache miss).
        """
        if not data:
            return None
        try:
            return pickle.loads(data)
        except Exception as ex:
            _logger.debug("Cannot unpickle value from Redis cache: \n%s", ex)
            return None

    @locked
    def __contains__(self, obj):
        if super().__contains__(obj):
            return True

        if self.dredis:
            return self.dredis.exists(self._get_redis_key(obj))

        return False

    @locked
    def __getitem__(self, obj):
        try:
            return super().__getitem__(obj)
        except KeyError as e:
            if self.dredis:
                try:
                    redis_key = self._get_redis_key(obj)
                    if (val := self.dredis.get(redis_key)) is not None:
                        if (decoded := self._decode_value(val)) is not None:
                            self.dredis.expire(redis_key, self.expiration)
                            return decoded
                        # Decode failed, delete corrupted cache entry
                        self.dredis.delete(redis_key)
                except Exception as ex:
                    _logger.debug("RedisLRU __getitem__ redis `get` error: \n%s", ex)
            raise e

    @locked
    def __setitem__(self, obj, val):
        super().__setitem__(obj, val)

        if self.dredis:
            encoded = self._encode_value(val)
            if encoded is None:
                # Value cannot be pickled, skip Redis storage (local cache still works)
                return
            try:
                redis_key = self._get_redis_key(obj)
                self.dredis.setex(redis_key, self.expiration, encoded)
            except Exception as ex:
                _logger.debug("RedisLRU __setitem__ redis `setex` error: \n%s", ex)

    @locked
    def __delitem__(self, obj):
        super().__delitem__(obj)

        if self.dredis:
            try:
                redis_key = self._get_redis_key(obj)
                self.dredis.delete(redis_key)
            except Exception as ex:
                _logger.debug("RedisLRU __delitem__ redis `delete` error: \n%s", ex)

    @locked
    def pop(self, key):
        res = super().pop(key)
        if self.dredis:
            try:
                redis_key = self._get_redis_key(key)
                if res is None:
                    if (val := self.dredis.get(redis_key)) is not None:
                        if (decoded := self._decode_value(val)) is not None:
                            res = decoded
                self.dredis.delete(redis_key)
            except Exception as ex:
                _logger.debug("RedisLRU pop redis `delete` error: \n%s", ex)
        return res

    @locked
    def clear(self):
        super().clear()
        self.clear_redis_cache()

    @locked
    def clear_redis_cache(self):
        if self.dredis:
            try:
                # scan_iter returns an iterator - processes keys in batches
                # Default batch size is 10, can increase with count parameter
                pattern = f"{self.prefix}*"
                keys_to_delete = []

                for key in self.dredis.scan_iter(match=pattern, count=100):
                    keys_to_delete.append(key)
                    # Delete in batches of 1000 to avoid huge delete commands
                    if len(keys_to_delete) >= 1000:
                        self.dredis.delete(*keys_to_delete)
                        keys_to_delete = []

                # Delete remaining keys
                if keys_to_delete:
                    self.dredis.delete(*keys_to_delete)

            except Exception as ex:
                _logger.debug("RedisLRU clear redis `delete` error: \n%s", ex)
