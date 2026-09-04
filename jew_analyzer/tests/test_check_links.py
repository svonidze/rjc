import csv
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR))

import check_links as checker


def row_with_columns(**values):
    row = [None] * 17
    for column, value in values.items():
        row[ord(column.upper()) - ord('A')] = value
    return row


class ExcelLayoutTests(unittest.TestCase):
    def test_mixed_layouts_preserve_rows_and_metadata(self):
        standard = row_with_columns(
            G='standard text', I='https://dzen.ru/a/standard',
            K='Комментарий', M='Standard Author')
        shifted = row_with_columns(
            F='shifted text', G='vk.com', H='https://vk.com/shifted',
            I='Соцсети', J='Пост', L='Shifted Author')
        header = row_with_columns(
            F='Заголовок', G='Текст', H='Источник', I='Url',
            K='Тип', M='Автор')
        empty = row_with_columns()

        records = checker._build_records({
            'Mixed': pd.DataFrame([standard, shifted, header, empty])})

        self.assertEqual([record['row'] for record in records], [2, 3])
        self.assertEqual([record['layout'] for record in records], ['G/I', 'F/H'])
        self.assertEqual(records[0]['type'], 'Комментарий')
        self.assertEqual(records[0]['author'], 'Standard Author')
        self.assertEqual(records[1]['type'], 'Пост')
        self.assertEqual(records[1]['author'], 'Shifted Author')
        self.assertEqual(records[1]['text'], 'shifted text')
        self.assertEqual(records[1]['url'], 'https://vk.com/shifted')

    def test_both_valid_layouts_prefer_standard_and_warn(self):
        ambiguous = row_with_columns(
            F='shifted text', G='standard text',
            H='https://vk.com/shifted', I='https://vk.com/standard',
            K='Комментарий', M='Standard Author')

        with self.assertLogs(checker.logger, level='WARNING') as captured:
            records = checker._build_records({
                'Ambiguous': pd.DataFrame([ambiguous])})

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['layout'], 'G/I')
        self.assertEqual(records[0]['text'], 'standard text')
        self.assertIn('using standard G/I', '\n'.join(captured.output))

    def test_invalid_populated_row_is_retained_for_error_report(self):
        invalid = row_with_columns(G='search text', I='not a URL')
        records = checker._build_records({'Invalid': pd.DataFrame([invalid])})
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['url'], 'not a URL')


class SafeWebStatusTests(unittest.TestCase):
    @staticmethod
    def fields(body):
        return {'message_fields': [], 'body_text': body}

    def test_dynamic_social_match_is_alive(self):
        status, error, match_type, snippet = checker.decide_nontelegram_web_status(
            'https://vk.com/post', 'https://vk.com/post',
            'искомый текст', self.fields('prefix искомый текст suffix'))
        self.assertEqual(status, checker.STATUS_ALIVE)
        self.assertIsNone(error)
        self.assertEqual(match_type, 'exact')
        self.assertEqual(snippet, 'искомый текст')

    def test_dynamic_social_non_match_is_error(self):
        status, error, match_type, snippet = checker.decide_nontelegram_web_status(
            'https://ok.ru/post', 'https://ok.ru/post',
            'искомый текст', self.fields('другое содержимое'))
        self.assertEqual(status, checker.STATUS_ERROR)
        self.assertIn('статическом HTML', error)
        self.assertIsNone(match_type)
        self.assertIsNone(snippet)

    def test_auth_redirect_is_error_with_destination(self):
        status, error, _, _ = checker.decide_nontelegram_web_status(
            'https://dzen.ru/a/post', 'https://sso.passport.yandex.ru/push',
            'искомый текст', self.fields('Войти'))
        self.assertEqual(status, checker.STATUS_ERROR)
        self.assertIn('sso.passport.yandex.ru', error)
        self.assertIn('требуется вход', error)

    def test_regular_site_non_match_remains_deleted(self):
        status, error, _, _ = checker.decide_nontelegram_web_status(
            'https://example.com/post', 'https://example.com/post',
            'искомый текст', self.fields('другое содержимое'))
        self.assertEqual(status, checker.STATUS_DELETED)
        self.assertIsNone(error)


class CsvReportTests(unittest.TestCase):
    def test_report_is_created_when_all_rows_are_errors(self):
        record = {
            'row': 2, 'type': 'Комментарий', 'author': 'Автор',
            'url': 'https://vk.com/post', 'text': 'текст',
            'status': checker.STATUS_ERROR, 'match_type': None,
            'snippet': 'нужна ручная проверка',
        }
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'results.csv'
            self.assertTrue(checker.write_results_csv([record], output))
            with output.open(encoding='utf-8-sig', newline='') as handle:
                rows = list(csv.DictReader(handle))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['Статус'], checker.STATUS_ERROR)
        self.assertEqual(rows[0]['НайденныйФрагмент'], 'нужна ручная проверка')


if __name__ == '__main__':
    unittest.main()
