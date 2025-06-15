

"""
Utility helpers per la gestione dei periodi temporali nelle query SQL.

La funzione principale è `bounds(time_type, time_value)` che converte la
rappresentazione logica usata dal parser (`time_type` / `time_value`) nei
bound `start` e `end` (datetime.date) da usare nella clausola
`BETWEEN start AND end`.

time_type  | time_value (esempio)              | Risultato
-----------|-----------------------------------|---------------------------------
latest     | None                              | (None, None)  -> gestito dal caller con ORDER BY
year       | "2023"                            | (2023‑01‑01, 2023‑12‑31)
month      | "2023-12"                         | (2023‑12‑01, 2023‑12‑31)
date       | "2023-12-15"                      | (2023‑12‑15, 2023‑12‑15)
range      | {"from":"2020","to":"2024"}       | (2020‑01‑01, 2024‑12‑31)

Eccezioni sollevate:
- ValueError se `time_type` non è riconosciuto o il formato `time_value` è invalido.
"""

import datetime
import calendar
from typing import Tuple, Optional, Dict, Any

__all__ = ["bounds"]

DatePair = Tuple[Optional[datetime.date], Optional[datetime.date]]


def _last_day_of_month(year: int, month: int) -> int:
    """Ritorna l'ultimo giorno di un dato mese."""
    return calendar.monthrange(year, month)[1]


def bounds(time_type: str, time_value: Any) -> DatePair:
    """
    Converte (time_type, time_value) in una coppia (start_date, end_date).

    Parameters
    ----------
    time_type : str
        "latest" | "year" | "month" | "date" | "range"
    time_value : Any
        Formato dipende dal type:
        - latest -> None
        - year   -> "YYYY"
        - month  -> "YYYY-MM"
        - date   -> "YYYY-MM-DD"
        - range  -> {"from":"YYYY","to":"YYYY"}

    Returns
    -------
    (start_date, end_date) : tuple[datetime.date | None, datetime.date | None]
        Se time_type == "latest" ritorna (None, None) perché verrà poi usato
        ORDER BY ... DESC LIMIT 1 nel layer SQL.
    """
    if time_type == "latest":
        return None, None

    if time_type == "year":
        try:
            y = int(time_value)
            return datetime.date(y, 1, 1), datetime.date(y, 12, 31)
        except (ValueError, TypeError):
            raise ValueError(f"Formato 'year' non valido: {time_value!r}")

    if time_type == "month":
        try:
            year_str, month_str = str(time_value).split("-")
            y, m = int(year_str), int(month_str)
            last_day = _last_day_of_month(y, m)
            return datetime.date(y, m, 1), datetime.date(y, m, last_day)
        except Exception:
            raise ValueError(f"Formato 'month' non valido: {time_value!r}")

    if time_type == "date":
        try:
            d = datetime.date.fromisoformat(time_value)
            return d, d
        except Exception:
            raise ValueError(f"Formato 'date' non valido: {time_value!r}")

    if time_type == "range":
        if not (isinstance(time_value, Dict) and "from" in time_value and "to" in time_value):
            raise ValueError("Per time_type 'range' serve un dict {'from': 'YYYY', 'to': 'YYYY'}")
        start_year = int(time_value["from"])
        end_year = int(time_value["to"])
        start = datetime.date(start_year, 1, 1)
        end = datetime.date(end_year, 12, 31)
        return start, end

    raise ValueError(f"time_type sconosciuto: {time_type!r}")