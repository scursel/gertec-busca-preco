#!/usr/bin/env python3
"""Regression tests for the Gertec price response formatter."""

import unittest
from pathlib import Path


# server.py creates its file logger during import.
(Path(__file__).parent / "logs").mkdir(exist_ok=True)

import server
from server import format_price_response


class PriceResponseTests(unittest.TestCase):
    @staticmethod
    def split_response(name, price):
        response = format_price_response(name, price).decode("ascii")
        name_field, price_field = response[1:].split("|", 1)
        return response, name_field, price_field

    def test_long_name_uses_all_four_display_lines_and_currency(self):
        response, name_field, price_field = self.split_response(
            "BISCOITO RECHEADO CHOCOLATE 130G", 12.9
        )

        self.assertEqual(len(name_field), 80)
        self.assertEqual(name_field[0:20].rstrip(), "BISCOITO RECHEADO")
        self.assertEqual(name_field[20:40].strip(), "CHOCOLATE 130G")
        self.assertEqual(price_field, "R$ 12,90")
        self.assertNotIn("\n", response)

    def test_name_longer_than_display_is_limited_to_four_lines(self):
        _, name_field, price_field = self.split_response(
            "NOME MUITO LONGO COM MAIS DE OITENTA CARACTERES PARA "
            "VALIDAR O LIMITE DO DISPLAY DO TERMINAL GERTEC",
            1234.5,
        )

        self.assertEqual(len(name_field), 80)
        self.assertIn("...", name_field[-20:])
        self.assertEqual(price_field, "R$ 1234,50")

    def test_missing_price_keeps_product_name_and_uses_fallback(self):
        _, name_field, price_field = self.split_response("PRODUTO SEM PRECO", None)

        self.assertEqual(name_field[:20].strip(), "PRODUTO SEM PRECO")
        self.assertEqual(price_field, "SEM PRECO")


class BarcodeLookupTests(unittest.TestCase):
    def test_ean13_zero_prefix_matches_upca_catalog_entry(self):
        product = {"nome": "ENERGY DRINK", "preco": 10.99}
        products = {"012345678905": product}

        finder = getattr(server, "find_product_by_barcode", None)
        if finder is None:
            self.fail(
                "find_product_by_barcode must support GTIN-12/GTIN-13 equivalence"
            )
        self.assertIs(finder(products, "0012345678905"), product)


if __name__ == "__main__":
    unittest.main()
