.. image:: https://img.shields.io/badge/licence-LGPL--3-green.svg
    :target: https://www.gnu.org/licenses/lgpl-3.0-standalone.html
    :alt: License: LGPL-3

Redis Session
==================================================================================================================
* Store sessions in Redis instead of the file system to increase Odoo performance.

Why Redis?
============
Redis (Remote Dictionary Server) is a fast, open-source, in-memory data structure store that serves a variety of use cases for enhancing the performance, scalability, and reliability of applications. Here are some reasons why use Redis:

1. High Performance and Low Latency:

   * Redis stores data in memory rather than on disk, leading to extremely fast read and write operations.
   * With sub-millisecond response times, Redis can handle high throughput and is often used in caching to minimize latency in applications.

2. Caching:

   * Redis is commonly used as a caching layer to reduce database load. Frequently accessed data, like user sessions.
   * It also supports automatic data expiration, which means data can expire after a certain time, freeing up memory and keeping the cache fresh.

3. Persistence Options:

   * Although Redis is primarily an in-memory database, it can be configured for persistence. You can save snapshots of the data periodically (RDB snapshots) or log every write (AOF logs), allowing data recovery after a crash.

Configuration
-------
1. Install and configure Redis:

   * sudo apt update
   * apt install redis
   * systemctl enable redis-server
   * systemctl start redis-server
   * redis-cli, CHECK REDIS
   * keys *

2. Install Redis Session module:

   * Python redis library: python3 -m pip install redis
   * Install Redis Session from Odoo applications

3. Configure Redis Session module in your odoo.conf file:

   * server_wide_modules = base,web,fpg_redis_session
   * redis_enable = True or False
   * redis_host = localhost or YOUR REDIS SERVER
   * redis_port = 6379 or YOUR REDIS PORT
   * redis_db_index = False or 0 IT's THE SAME or DB INDEX
   * redis_prefix = session or OTHER WORD
   * redis_session_expiration = 604800, DEFAULT 7 DAYS (60 * 60 * 24 * 7)
   * redis_session_expiration_anon = 86400, DEFAULT 24 HOURS (60 * 60 * 24)

License
-------
General Public License, Version 3 (LGPL v3).
(https://www.gnu.org/licenses/lgpl-3.0-standalone.html)

Contacts
--------
Mail Contact : odooapps24@gmail.com

Further information
===================
HTML Description: `<static/description/index.html>`__