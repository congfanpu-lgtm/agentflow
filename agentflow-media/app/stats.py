from dataclasses import dataclass


@dataclass
class Stats:
    """Times real work was done since process start. Cache hits don't count."""
    downloads: int = 0
    asr_runs: int = 0
    ocr_runs: int = 0
