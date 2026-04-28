# -*- coding: utf-8 -*-

import pickle
import redis

from odoo.tools._vendor.sessions import SessionStore
from odoo import tools

from odoo.service import security


DEFAULT_SESSION_TIMEOUT      = 60 * 60 * 24 * 7 # 7  Days  (seconds)
DEFAULT_SESSION_TIMEOUT_ANON = 60 * 60 * 24     # 24 Hours (seconds)


class RedisSessionStore(SessionStore):
    """Redis :: where to load and save session objects."""

    def __init__(self, session_class=None) -> None:
        super().__init__(session_class=session_class)

        # Redis params
        self._prefix = tools.config.get('redis_prefix', 'session')
        self._expiration = tools.config.get('redis_session_expiration', DEFAULT_SESSION_TIMEOUT)
        self._expiration_anon = tools.config.get('redis_session_expiration_anon', DEFAULT_SESSION_TIMEOUT_ANON)

        # Create redis instance
        self._redis_connect()

    def save(self, session):
        """Save a session."""
        key = self._get_session_key(sid=session.sid)
        data = pickle.dumps(dict(session), protocol=pickle.HIGHEST_PROTOCOL)
        expiration = self._get_expiration(session=session)
        return self._redis.set(name=key, value=data, ex=expiration)

    def delete(self, session):
        """Delete a session."""
        key = self._get_session_key(sid=session.sid)
        self._redis.delete(key)

    def get(self, sid):
        """Get a session for this sid or a new session object.
        This method has to check if the session key is valid and create 
        a new session if that wasn't the case."""
        key = self._get_session_key(sid=sid)
        data = self._redis.get(key)
        if data:
            self._redis.set(name=key, value=data, ex=self._expiration)  # Extend expiration
            data = pickle.loads(data)
        else:
            data = {}
        return self.session_class(data, sid, False)

    def rotate(self, session, env):
        """Rotate a session."""
        self.delete(session)
        session.sid = self.generate_key()
        if session.uid and env:
            session.session_token = security.compute_session_token(session, env)
        session.should_rotate = False
        self.save(session)

    def vacuum(self, max_lifetime):
        """No need to GC."""

    def list(self):
        """Lists all sessions in the store."""
        keys = self._redis.keys(f'{self._prefix}*')
        return [key[len(self._prefix):] for key in keys]

    def _get_session_key(self, sid):
        """Format session"""
        return f'{self._prefix}:{sid}'

    def _get_expiration(self, session):
        """Get expiration bases on the session existency"""
        if session.sid:
            return session.expiration or self._expiration
        return session.expiration or self._expiration_anon

    def _redis_connect(self):
        """Connect to Redis and get the instance"""
        params = {
            'host': tools.config.get('redis_host', '127.0.0.1'),
            'port': tools.config.get('redis_port', 6379),
        }
        db_index = tools.config.get('redis_db_index', False)
        if db_index:
            params['db'] = db_index
        try:
            self._redis = redis.Redis(**params)
            self._redis.ping()
        except redis.ConnectionError as e:
            raise redis.ConnectionError(e.args) from e
