class ScraperError(Exception):
    """Base exception for scraping errors."""
    pass

class InvalidPlayerDataError(ValueError):
    """Raised when player data is invalid."""
    pass

class DatabaseConnectionError(Exception):
    """Raised when the database connection fails."""
    pass

class ExportError(Exception):
    """Raised when export to CSV fails."""
    pass
