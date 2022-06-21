import datetime
import psycopg2

from wal_e.exception import UserException


class UTC(datetime.tzinfo):
    """
    UTC timezone

    Adapted from a Python example

    """

    ZERO = datetime.timedelta(0)
    HOUR = datetime.timedelta(hours=1)

    def utcoffset(self, dt):
        return self.ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return self.ZERO


class PgBackupStatements(object):
    """
    Contains operators to start and stop a backup on a Postgres server

    """

    def __init__(self):
        self._conn = psycopg2.connect(dbname='postgres')
        self._conn.autocommit = True
        with self._conn.cursor() as cur:
            cur.execute('SET statement_timeout=0')

    @property
    def _wal_name(self):
        return 'wal' if self._conn.server_version >= 100000 else 'xlog'

    def run_start_backup(self):
        """
        Connects to a server and attempts to start a hot backup

        Yields the WAL information in a dictionary for bookkeeping and
        recording.

        """

        # The difficulty of getting a timezone-stamped, UTC,
        # ISO-formatted datetime is downright embarrassing.
        #
        # See http://bugs.python.org/issue5094
        label = 'freeze_start_' + (datetime.datetime.utcnow()
                                   .replace(tzinfo=UTC()).isoformat())

        try:
            with self._conn.cursor() as cur:
                if self._conn.server_version >= 150000:
                    cur.execute(("SELECT file_name, lpad(file_offset::text, 8, '0') AS file_offset"
                                " FROM pg_{0}file_name_offset(pg_backup_start(%s, false))")
                                .format(self._wal_name), (label,))
                else:
                    cur.execute(("SELECT file_name, lpad(file_offset::text, 8, '0') AS file_offset"
                                " FROM pg_{0}file_name_offset(pg_start_backup(%s, false, false))")
                                .format(self._wal_name), (label,))
                return dict(list(zip(['file_name', 'file_offset'], cur.fetchone())))
        except Exception:
            raise UserException('Could not start hot backup')

    def run_stop_backup(self):
        """
        Stop a hot backup, if it was running, or error

        Return the last WAL file name and position that is required to
        gain consistency on the captured heap.

        """

        try:
            with self._conn.cursor() as cur:
                if self._conn.server_version >= 150000:
                    cur.execute(("SELECT file_name, lpad(file_offset::text, 8, '0') AS file_offset,"
                                " labelfile, spcmapfile FROM (SELECT (pg_{0}file_name_offset(lsn)).*,"
                                " labelfile, spcmapfile FROM pg_backup_stop()) a").format(self._wal_name))
                else:
                    cur.execute(("SELECT file_name, lpad(file_offset::text, 8, '0') AS file_offset,"
                                " labelfile, spcmapfile FROM (SELECT (pg_{0}file_name_offset(lsn)).*,"
                                " labelfile, spcmapfile FROM pg_stop_backup(false)) a").format(self._wal_name))
                return dict(list(zip(['file_name', 'file_offset', 'labelfile', 'spcmapfile'], cur.fetchone())))
        except Exception:
            raise UserException('Could not stop hot backup')

    def pg_version(self):
        """
        Get a very informative version string from Postgres

        Includes minor version, major version, and architecture, among
        other details.

        """
        with self._conn.cursor() as cur:
            cur.execute('SELECT * FROM version()')
            return dict(list(zip(['version'], cur.fetchone())))
