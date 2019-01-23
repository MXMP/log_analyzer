from unittest import TestCase
from datetime import date

import log_analyzer


class TestLogAnalyzer(TestCase):
    def test_median(self):
        numbers = [1, 5, 7, 10, 2, 5, 7]
        self.assertEqual(log_analyzer.median(numbers), 5)

    def test_get_last_log_file(self):
        last_log_file = log_analyzer.LogfileInfo(filename='nginx-access-ui.log-20170630',
                                                     date=date(2017, 6, 30))
        self.assertEqual(log_analyzer.get_last_log_file('./fake_logs'), last_log_file)

    def test_parse_line(self):
        input_line = '1.200.76.128 f032b48fb33e1e692  - [29/Jun/2017:03:50:32 +0300] "GET /api/1/campaigns/?id=617832 HTTP/1.1" 200 637 "-" "-" "-" "1498697432-4102637017-4709-9928915" "-" 0.146'
        bad_line = '1.200.76.128 f032b48fb33e1e692  - [29/Jun/2017:03:50:327432-4102637017-4709-9928915" "-" 0.146'
        parsed = {'status': '200',
                  'body_bytes_sent': '637',
                  'remote_user': 'f032b48fb33e1e692',
                  'request_time': 0.146,
                  'http_referer': '"-"',
                  'remote_addr': '1.200.76.128',
                  'url': '/api/1/campaigns/?id=617832',
                  'http_X_RB_USER': '"-"',
                  'request': '"GET /api/1/campaigns/?id=617832 HTTP/1.1"',
                  'http_x_forwarded_for': '"-"',
                  'http_user_agent': '"-"',
                  'time_local': '[29/Jun/2017:03:50:32 +0300]',
                  'http_X_REQUEST_ID': '"1498697432-4102637017-4709-9928915"',
                  'http_x_real_ip': '-'}
        self.assertEqual(log_analyzer.parse_line(input_line, log_analyzer.LOG_FORMAT), parsed)
        self.assertIsNone(log_analyzer.parse_line(bad_line, log_analyzer.LOG_FORMAT))
