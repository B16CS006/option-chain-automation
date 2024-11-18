from datetime import datetime

def time_round_down_to_nearest_internal(t: datetime, interval: int):
  return t.replace(minute=(t.minute // interval) * interval, second=0, microsecond=0)
