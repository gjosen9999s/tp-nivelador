import tempfile
import threading

from lottery.bet import Bet
from lottery.lottery import Lottery


class Coordinator:
    def __init__(self, agency_quorum_min: int) -> None:
        self._agency_quorum_min = agency_quorum_min
        self._storage = tempfile.NamedTemporaryFile()
        self._lottery = Lottery(self._storage.name)
        self._storage_lock = threading.Lock()
        self._cond = threading.Condition()
        self._finished_agencies: set[int] = set()
        self._draw_ready = False

    def store_bets(self, bets: list[Bet]) -> None:
        with self._storage_lock:
            self._lottery.store_bets(bets)

    def register_agency(self, agency_ids: set[int]) -> None:
        with self._cond:
            self._finished_agencies.update(agency_ids)
            if len(self._finished_agencies) >= self._agency_quorum_min:
                # Hay quorum, se dispara el "sorteo"
                self._draw_ready = True
                self._cond.notify_all()
            while not self._draw_ready:
                self._cond.wait()

    def winners_for(self, agency_ids: set[int]) -> list[Bet]:
        with self._storage_lock:
            return [
                bet
                for bet in self._lottery.load_bets()
                if self._lottery.has_won(bet) and bet.agency_id in agency_ids
            ]

    def shutdown(self) -> None:
        # si el quorum nunca se completa, el shutdown libera igualmente a los handlers
        with self._cond:
            self._draw_ready = True
            self._cond.notify_all()
