from typing import Callable, Dict, Optional, Union, Set
from dataclasses import dataclass
from functools import partial

Signature = Union[str, Set[str]]


@dataclass(frozen=True)
class Scheme:
    name: str
    n: Optional[int] = None

    def __call__(self, value: str) -> Signature:
        return FUNC[self](value)


def get_first_nchars(value: str, n: int) -> str:
    return value[:n]


def get_last_nchars(value: str, n: int) -> str:
    return value[-n:]


def get_ngrams(value: str, n: int) -> Set[str]:
    return set(value[i : i + n] for i in range(len(value) - n + 1))


def get_acronym(value: str) -> str:
    return "".join(word[0] for word in value.split())


FUNC: Dict[Scheme, Callable[[str], Signature]] = {
    **{
        Scheme("first_nchars", n): partial(get_first_nchars, n=n)
        for n in {2, 4, 6}
    },
    **{
        Scheme("last_nchars", n): partial(get_last_nchars, n=n)
        for n in {2, 4, 6}
    },
    **{Scheme("find_ngrams", n): partial(get_ngrams, n=n) for n in {4, 6, 8}},
    Scheme("acronym"): get_acronym,
    Scheme("exactmatch"): lambda value: value,
}
