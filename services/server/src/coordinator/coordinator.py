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
        self._finished_agencies = 0
        self._draw_ready = False

    def store_bets(self, bets: list[Bet]) -> None:
        with self._storage_lock:
            self._lottery.store_bets(bets)

    def register_agency(self) -> None:
        with self._cond:
            self._finished_agencies += 1
            if self._finished_agencies >= self._agency_quorum_min:
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