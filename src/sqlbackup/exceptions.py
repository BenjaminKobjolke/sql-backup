"""Custom exception hierarchy."""


class SqlBackupError(Exception):
    """Base exception for sqlbackup."""


class ConfigError(SqlBackupError):
    """Configuration-related errors."""


class ConnectionError(SqlBackupError):
    """Database connection errors."""


class BackupError(SqlBackupError):
    """Backup operation errors."""


class PushError(SqlBackupError):
    """Push/restore operation errors."""
