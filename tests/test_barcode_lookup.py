import unittest

import server


class BarcodeLookupTests(unittest.TestCase):
    def test_ean13_with_zero_prefix_matches_upca_in_catalog(self):
        product = {
            "nome": "MONSTER ABSOLUTELY ZERO 473ML",
            "produtoCodigo": 1581218,
            "preco": 10.99,
        }
        products = {"070847022305": product}

        finder = getattr(server, "find_product_by_barcode", None)
        self.assertIsNotNone(
            finder,
            "find_product_by_barcode must support GTIN-12/GTIN-13 equivalence",
        )
        self.assertIs(finder(products, "0070847022305"), product)

    def test_parse_persisted_hit_keeps_date_and_price(self):
        parser = getattr(server, "_parse_query_log_line", None)
        self.assertIsNotNone(parser)

        query = parser(
            "2026-08-20 22:14:11,298 [INFO] "
            "HIT: 7891991001342 -> GUARANA 2LTS R$13.99",
            "histórico",
        )

        self.assertEqual(query["time"], "2026-08-20T22:14:11.298")
        self.assertEqual(query["barcode"], "7891991001342")
        self.assertEqual(query["nome"], "GUARANA 2LTS")
        self.assertEqual(query["preco"], 13.99)
        self.assertTrue(query["hit"])

    def test_parse_persisted_miss_is_visible_as_miss(self):
        query = server._parse_query_log_line(
            "2026-07-29 16:43:53,143 [INFO] MISS: 0000000000000",
            "histórico",
        )

        self.assertEqual(query["time"], "2026-07-29T16:43:53.143")
        self.assertEqual(query["barcode"], "0000000000000")
        self.assertFalse(query["hit"])
        self.assertIsNone(query["nome"])


if __name__ == "__main__":
    unittest.main()
