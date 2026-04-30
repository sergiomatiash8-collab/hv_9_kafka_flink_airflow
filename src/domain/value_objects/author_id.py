from dataclasses import dataclass

@dataclass(frozen=True)
class AuthorId:
    value: str

    def __post_init__(self):
        if not self.value or not isinstance(self.value, str):
            raise ValueError("AuthorId must be a non-empty string")

    def __str__(self):
        return self.value