"""Data models for CS2 demo pipeline."""
from dataclasses import dataclass, field, asdict
from typing import Optional, List


@dataclass
class MVPEvent:
    event_id: int
    name: str
    winner: Optional[str] = None
    maps_count: Optional[int] = None
    results_url: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None

    def to_dict(self):
        return asdict(self)


@dataclass
class Match:
    match_id: int
    event_id: int
    event_name: str
    team1: str
    team2: str
    score1: int
    score2: int
    date: str
    format: str  # e.g. 'bo3', 'bo1', 'bo5'
    url: str
    demo_id: Optional[int] = None
    demo_url: Optional[str] = None
    maps: List[str] = field(default_factory=list)

    def to_dict(self):
        return asdict(self)
