from typing import Literal, NewType

Stage = Literal["extract", "transform", "load"]

SourceName = NewType("SourceName", str)
SourceUrl = NewType("SourceUrl", str)
BronzeId = NewType("BronzeId", int)
SilverId = NewType("SilverId", int)
