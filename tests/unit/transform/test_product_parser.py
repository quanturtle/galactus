import pytest

from galactus.transform.product_parser import ProductParser


class _Concrete(ProductParser):
    """Minimal concrete subclass — the helper under test doesn't touch the
    abstract extract_* methods, so they can stay unimplemented for the tests."""

    source = "test"

    def extract_source_url(self, item):  # type: ignore[override]
        return ""

    def extract_sku(self, item):  # type: ignore[override]
        return None

    def extract_name(self, item):  # type: ignore[override]
        return ""

    def extract_brand(self, item):  # type: ignore[override]
        return None

    def extract_price(self, item):  # type: ignore[override]
        return None

    def extract_currency(self, item):  # type: ignore[override]
        return ""

    def extract_unit(self, item):  # type: ignore[override]
        return None

    def extract_image_urls(self, item):  # type: ignore[override]
        return []


@pytest.fixture
def parser() -> _Concrete:
    return _Concrete()


@pytest.mark.parametrize(
    "name,expected",
    [
        # kg variants — bulk meats and produce
        ("BOGA X KILO", "kg"),
        ("UPISA CHORIZO TOSCANO X KG", "kg"),
        ("Carne Vacio Biggie x kg.", "kg"),
        ("BUDIN DE ZANAHORIA X KG.", "kg"),
        ("LOCOTE ROJO IMPORTADO X KG.", "kg"),
        # liter variants — including Paraguayan decimal styles
        ("LECHE LA SERENISIMA ENTERA UAT 1LT", "l"),
        ("JABON LIQ OMO ULTRA POWER P/ROPAS 1.8 LT", "l"),
        ("JUGO SABOR DURAZNO PURO SOL 1LT", "l"),
        ("GASEOSA SIN AZUCAR DESCARTABLE COCA COLA 1.5LT", "l"),
        # milliliter variants — including 2.000 ml. Paraguayan thousand separator
        ("Gaseosa Coca Cola Original de 2.000 ml.", "ml"),
        ("ENJUAGUE BUCAL COLGATE PLAX SANDÍA 250ML", "ml"),
        ("ENJUAGUE BUCAL COLGATE PLAX ICE INFINTY 500 ML.", "ml"),
        ("ACONDICIONADOR DOVE HIDRATACION INTENSA 200 ML", "ml"),
        ("Aceite de almendras en frasco Carey 60 ml", "ml"),
        # gram variants — both G/GR/GRS suffixes
        ("DESODORANTE SPEED STICK XTREME CREMA HOMBRE 70G", "g"),
        ("CREMA DENTAL COLGATE LUMIN WHITE FRESH 70G", "g"),
        ("MARGARINA DELICATA VEGETAL CREMOSA 250GR", "g"),
        ("CAFE NESCAFE CLASICO 8 GRS.", "g"),
        ("Crema dental triple acción Colgate 180 gramos", "g"),
        # cc variant — rare per-oil case
        ("ACEITE DE OLIVA ROCIO NATUR S/GLUTEN 120CC", "cc"),
        # bundles and counts — no measurable unit, must stay None
        ("Lechuga Hidroponica Biggie x Mazo.", None),
        ("PAPEL HIG. BIO DOBLE HOJA NEUTRO X 12UN (8)", None),
        ("Jabones carbón détox Protex 3 unidades", None),
        # empty / no-unit names
        ("", None),
        ("PRODUCTO SIN UNIDADES", None),
    ],
)
def test_parse_unit_from_name(parser: _Concrete, name: str, expected: str | None) -> None:
    assert parser.parse_unit_from_name(name) == expected
