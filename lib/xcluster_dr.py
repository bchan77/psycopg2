# psycopg2/xcluster_dr.py - xCluster DR connection pool
#
# Copyright (C) 2025 YugabyteDB, Inc.
#
# xCluster DR support: connection pool that automatically routes to the
# active (source) cluster in an xCluster DR pair, with failover detection
# and automatic swap when the active cluster changes.
#
# Requires: database must be part of xCluster DR replication with automatic
# DDL mode (yb_xcluster_ddl_replication extension present).

"""xCluster DR connection pool for YugabyteDB.

Routing to the active cluster in an xCluster DR setup, with automatic
failover detection and pool swap.
"""

import threading
import time

import psycopg2
from psycopg2 import pool
from psycopg2 import logger


REPLICATION_ROLE_QUERY = "SELECT yb_xcluster_ddl_replication.get_replication_role()"


def _get_replication_role(conn):
    """Return 'source' (active), 'target' (passive), or None on error."""
    try:
        with conn.cursor() as cur:
            cur.execute(REPLICATION_ROLE_QUERY)
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as e:
        logger.warning("get_replication_role failed: %s", e)
        return None


def _is_active_pool(pool_obj):
    """True if pool is connected to active (source) cluster."""
    conn = pool_obj.getconn()
    try:
        role = _get_replication_role(conn)
        return role == "source"
    except Exception:
        return False
    finally:
        pool_obj.putconn(conn)


class XClusterDRPool:
    """Connection pool that routes to the active cluster in an xCluster DR pair.

    Maintains two pools (one per cluster), detects which is active via
    yb_xcluster_ddl_replication.get_replication_role(), and routes
    getconn() to the active pool. A background health check swaps pools
    on failover.

    Usage::

        pool = psycopg2.XClusterDRPool(
            cluster_a_dsn="host=a1 port=5433 dbname=db user=u password=p",
            cluster_b_dsn="host=b1 port=5433 dbname=db user=u password=p",
            minconn=2,
            maxconn=10,
            health_check_interval=30,
        )
        conn = pool.getconn()
        try:
            # use conn
        finally:
            pool.putconn(conn)
        pool.closeall()
    """

    def __init__(
        self,
        cluster_a_dsn,
        cluster_b_dsn,
        minconn=1,
        maxconn=20,
        health_check_interval=30,
        **connect_kwargs
    ):
        """
        Args:
            cluster_a_dsn: Connection string for cluster A.
            cluster_b_dsn: Connection string for cluster B.
            minconn: Minimum connections per pool.
            maxconn: Maximum connections per pool.
            health_check_interval: Seconds between role checks.
            **connect_kwargs: Extra arguments passed to psycopg2.connect().
        """
        self._pool_a = pool.ThreadedConnectionPool(
            minconn, maxconn, cluster_a_dsn, **connect_kwargs
        )
        self._pool_b = pool.ThreadedConnectionPool(
            minconn, maxconn, cluster_b_dsn, **connect_kwargs
        )
        self._health_interval = health_check_interval
        self._lock = threading.Lock()
        self._active_pool = None
        self._passive_pool = None
        self._active_name = None
        self._passive_name = None
        self._closed = False
        self._stop_health = threading.Event()

        self._init_roles()
        self._start_health_check()

    def _init_roles(self):
        """Determine active vs passive from get_replication_role()."""
        conn_a = self._pool_a.getconn()
        conn_b = self._pool_b.getconn()
        try:
            role_a = _get_replication_role(conn_a)
            role_b = _get_replication_role(conn_b)
        finally:
            self._pool_a.putconn(conn_a)
            self._pool_b.putconn(conn_b)

        if role_a == "source" and role_b == "target":
            self._active_pool = self._pool_a
            self._passive_pool = self._pool_b
            self._active_name = "A"
            self._passive_name = "B"
        elif role_b == "source" and role_a == "target":
            self._active_pool = self._pool_b
            self._passive_pool = self._pool_a
            self._active_name = "B"
            self._passive_name = "A"
        else:
            raise pool.PoolError(
                "xCluster DR: expected one source and one target. "
                "Got A=%r, B=%r. Ensure database is in xCluster DR replication."
                % (role_a, role_b)
            )

    def _rebuild_on_failover(self):
        """Swap active/passive if current active is now standby."""
        with self._lock:
            if self._closed or not self._active_pool:
                return
            if _is_active_pool(self._active_pool):
                return

            logger.warning("xCluster DR: active pool is now standby (failover). Switching.")
            self._active_pool, self._passive_pool = self._passive_pool, self._active_pool
            self._active_name, self._passive_name = self._passive_name, self._active_name
            logger.info("xCluster DR: Cluster %s now active, Cluster %s now passive",
                        self._active_name, self._passive_name)

    def _health_loop(self):
        while not self._stop_health.wait(self._health_interval):
            self._rebuild_on_failover()

    def _start_health_check(self):
        t = threading.Thread(target=self._health_loop, daemon=True)
        t.start()

    def getconn(self):
        """Get a connection from the active cluster pool."""
        with self._lock:
            if self._closed:
                raise pool.PoolError("xCluster DR pool is closed")
            p = self._active_pool
        return p.getconn()

    def putconn(self, conn, key=None, close=False):
        """Return a connection to the pool."""
        with self._lock:
            p = self._active_pool
        p.putconn(conn, key=key, close=close)

    def closeall(self):
        """Close all connections and stop the health check."""
        self._stop_health.set()
        with self._lock:
            self._closed = True
        for p in (self._pool_a, self._pool_b):
            try:
                p.closeall()
            except Exception:
                pass

    @property
    def active_cluster_name(self):
        """Name of the current active cluster ('A' or 'B')."""
        with self._lock:
            return self._active_name


