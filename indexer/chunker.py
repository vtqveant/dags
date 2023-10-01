import re
from itertools import tee, islice
from typing import Iterator, List


class Chunker:
    """
    Generator for lazy reading overlapping chunks from a string
    """

    def __init__(self, text: str, chunk_size):
        self._text = text
        self._chunk_size = chunk_size

    def get_chunks(self) -> Iterator[List[str]]:
        it1, it2 = tee(self._group(self._split(), self._chunk_size // 2))
        next(it2, None)
        for first, second in zip(it1, it2):
            yield first + second

    def _split(self, sep="\s+"):
        # s. https://stackoverflow.com/a/9770397
        return (_.group(1) for _ in re.finditer(f'(?:^|{sep})((?:(?!{sep}).)*)', self._text))

    @staticmethod
    def _group(iterator: Iterator, n: int) -> Iterator[list]:
        while chunk := list(islice(iterator, n)):
            yield chunk
