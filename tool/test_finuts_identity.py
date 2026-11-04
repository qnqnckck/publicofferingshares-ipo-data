import unittest
from validate_finuts_schedule_sync import normalize_company_key

class ExactAliasTests(unittest.TestCase):
    def test_reviewed_shinyoung_alias(self):
        self.assertEqual(normalize_company_key('신영스팩12호'), normalize_company_key('신영해피투모로우제12호스팩'))

    def test_different_series_remain_different(self):
        self.assertNotEqual(normalize_company_key('신영스팩11호'), normalize_company_key('신영해피투모로우제12호스팩'))

    def test_similar_issuers_are_not_merged(self):
        self.assertNotEqual(normalize_company_key('영광'), normalize_company_key('영광와이케이엠씨'))
        self.assertNotEqual(normalize_company_key('에이치엘지노믹스'), normalize_company_key('알지노믹스'))

if __name__ == '__main__':
    unittest.main()
