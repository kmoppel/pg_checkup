#!/usr/bin/env python

import logging
import time

import psycopg2
import psycopg2.extras
import argparse
import os
import sys

args = None
logger = None

SQL_GET_SNAPSHOT = """
    insert into public.stat_activity_history
      select now(), *
      from pg_stat_activity
      where pid != pg_backend_pid()
      and state != 'idle'
      and backend_type = 'client backend'
      and (select count(*) from pg_stat_activity
          where pid != pg_backend_pid()
          and backend_type = 'client backend') > 10 /* only when more than 10 client connections */
"""

SQL_DROP_SNAPSHOT_TABLE = """
    drop table if exists public.stat_activity_history;
"""

SQL_CREATE_SNAPSHOT_TABLE = """
    create {} table if not exists public.stat_activity_history
      as select now(), * from pg_stat_activity where false;
      /*
          NB! Before analyzing the snapshot data it might be a good idea to throw an index
          on the now column:
          create index on public.stat_activity_history (now);
      */
"""


def get_monitored_db_connection(autocommit=True):
    """ if password is in use, .pgpass must be used """
    conn = psycopg2.connect(host=args.host, port=args.port, database=args.dbname, user=args.username)
    if autocommit:
        conn.autocommit = True
    return conn


def get_snapshot_db_connection(autocommit=True):
    """ if password is in use, .pgpass must be used """
    conn = psycopg2.connect(host=args.snapshot_host, port=args.snapshot_port, database=args.snapshot_dbname, user=args.snapshot_username)
    if autocommit:
        conn.autocommit = True
    return conn


def try_close_connection(conn):
    if conn:
        try:
            conn.close()
        except:
            logging.exception('failed to close db connection')


def execute(conn, sql, params=None, statement_timeout=None, dml=False):
    result = []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if statement_timeout:
            cur.execute("SET statement_timeout TO '{}'".format(
                statement_timeout))
        cur.execute(sql, params)
        if not dml and (cur.statusmessage.startswith('SELECT') or cur.description):
            result = cur.fetchall()
        else:
            result = [{'rows_affected': str(cur.rowcount)}]
    except Exception as e:
        logging.exception('failed to execute "{}"'.format(sql))
        return result, str(e)
    return result, None


def exitOnErrormsg(errmsg, extra_description=None):
    if errmsg:
        if extra_description:
            logger.error("%s: %s", extra_description, errmsg)
        else:
            logger.error(errmsg)
        sys.exit(1)


def main():
    argp = argparse.ArgumentParser(description='Script that stores snapshots active sessions for later problem analysus. Additionally user specified filters can be applied')
    argp.add_argument('-q', '--quiet', help='Errors only', action='store_true')
    argp.add_argument('-d', '--delay', help='Sleep delay in milliseconds between storing snapshots. Default: 100', type=int, default=100, metavar='DELAY_MILLIS')
    argp.add_argument('-s', '--snapshot-db', help='Store snapshots not in monitored DB but in DB specified by --snapshot-* params', action='store_true')    # TODO use copy?
    argp.add_argument('-t', '--timeout', help='Monitor for given seconds and then exit. Default - no limit', type=int, metavar='TIMEOUT_SECONDS')
    argp.add_argument('-c', '--recreate', help='Drop and recreate the snapshot storing table (if exists), otherwise append', action='store_true')
    argp.add_argument('-l', '--logged', help='Create a normal, WAL logged, persistent table for storing snapshots', action='store_true')
    argp.add_argument('--min-connections', help='Only store snapshots when there are at least so many sessions', type=int, default=0)

    argp.add_argument('--host', help='Monitored DB host', required=True)
    argp.add_argument('--port', help='Monitored DB port', type=int, default='5432')
    argp.add_argument('--dbname', help='Monitored DB name', default='postgres')    # to snapshot only some DB-s alter the SQL_GET_SNAPSHOT query
    argp.add_argument('--username', help='Monitored DB username', default=os.getenv('USER'))

    argp.add_argument('--snapshot-host', help='Snapshot DB hostname', default='localhost')
    argp.add_argument('--snapshot-port', help='Snapshot DB port', type=int, default='5432')
    argp.add_argument('--snapshot-dbname', help='Snapshot DB name', default='postgres')
    argp.add_argument('--snapshot-username', help='Snapshot DB username', default=os.getenv('USER'))

    global args
    args, unknown_args = argp.parse_known_args()

    if args.snapshot_db:
        logging.error('--snapshot-db not yet implemented')
        exit(-1)

    logging.basicConfig(format='%(levelname)s %(message)s')
    global logger
    logger = logging.getLogger()
    logger.setLevel((logging.ERROR if args.quiet else logging.INFO))

    logger.info(args)

    conn_md = get_monitored_db_connection()
    if conn_md or exit(-1):
        logger.info('connection to monitored DB ok')
    conn_sn = None
    if args.snapshot_db:
        conn_sn = get_snapshot_db_connection()
        if conn_sn or exit(-1):
            logger.info('connection to snapshot DB ok')

    if args.recreate:
        logger.info('dropping the snapshot storage table...')
        _, err = execute(conn_sn if args.snapshot_db else conn_md, SQL_DROP_SNAPSHOT_TABLE)
        exitOnErrormsg(err, '')

    logger.info('creating the snapshot storage table if not existing...')
    _, err = execute(conn_sn if args.snapshot_db else conn_md, SQL_CREATE_SNAPSHOT_TABLE.format('unlogged' if not args.logged else ''), dml=True)
    exitOnErrormsg(err, '')

    logger.info('starting the main loop for ' + ((str(args.timeout) + 's') if args.timeout else 'indefinitely...'))

    start_time = time.time()
    while True:
        if args.timeout and time.time() - start_time > args.timeout:
            logger.info('exiting as --timeout reached')
            try_close_connection(conn_md)
            try_close_connection(conn_sn)
            exit(0)

        _, err = execute(conn_md, SQL_GET_SNAPSHOT)
        exitOnErrormsg(err, '')

        time.sleep(args.delay / 1000.0)


if __name__ == '__main__':
    main()
