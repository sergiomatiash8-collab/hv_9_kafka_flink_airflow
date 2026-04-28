from dataclasses import dataclass

@dataclass(frozen=True)
class AuthorId:
    value: str

    def __post_init__(self):
        if not self.value or not isinstance(self.value, str):
            raise ValueError("AuthorId має бути непустим рядком")

    def __str__(self):
        return self.value