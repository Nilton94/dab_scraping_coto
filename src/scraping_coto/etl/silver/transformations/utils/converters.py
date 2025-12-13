from pyspark.sql.functions import col, from_utc_timestamp, from_unixtime, when
from modules.utils.logger import get_logger

logger = get_logger(__name__)

def from_unixtime_to_timestamp(column, timezone: str = 'America/Sao_Paulo'):
    '''
        Returns a timestamp column from a unixtime column based on a timezone.
        Args:
            column: Column to be converted.
            timezone: Timezone to be used. Default is 'America/Sao_Paulo'.
        Returns:
            Timestamp column.
    '''

    logger.info(f"Converting unixtime column {column} to timestamp with timezone: {timezone}")

    if isinstance(column, str):
        column = col(column).cast('bigint')
    else:
        column = column.cast('bigint')

    return (
          when(
            column < 1e10, 
            from_utc_timestamp(from_unixtime(column), timezone)
          )
          .when(
            column < 1e13,
            from_utc_timestamp(from_unixtime(column / 1000), timezone)
          )
          .when(
            column < 1e16,
            from_utc_timestamp(from_unixtime(column / 1_000_000), timezone)
          )
          .otherwise(
            from_utc_timestamp(from_unixtime(column / 1_000_000_000), timezone)
          )
    )
