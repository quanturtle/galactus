from galactus.core.registry import ClassRegistry
from galactus.transform.base import Parser

PARSERS = ClassRegistry[Parser]("parser", base=Parser)
