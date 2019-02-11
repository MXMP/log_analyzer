#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import gzip
import os
import re
import logging
import json
from datetime import datetime
from string import Template
from collections import namedtuple


SEPARATOR_PATTERN = re.compile(r'\s+')
QUOTED_PATTERN = re.compile(r'"([^"]+)"')
SQUARE_PATTERN = re.compile(r'\[([^\]]+)\]')
STRING_PATTERN = re.compile(r'([^\s]+)')

LOG_FORMAT = 'remote_addr  remote_user http_x_real_ip [time_local] "request" status body_bytes_sent ' \
             '"http_referer" "http_user_agent" "http_x_forwarded_for" "http_X_REQUEST_ID" "http_X_RB_USER" ' \
             'request_time'

LogfileInfo = namedtuple('LogfileInfo', ['filename', 'date'])


def get_last_log_file(log_dir):
    if not os.path.isdir(log_dir):
        return None

    last_log = None
    for filename in os.listdir(log_dir):
        match = re.match(r'^nginx-access-ui\.log-(?P<date>\d{8})(\.gz)?$', filename)
        try:
            file_date = datetime.strptime(match.groupdict()['date'], '%Y%m%d').date()
        except (ValueError, AttributeError):
            continue

        if not last_log or file_date > last_log.date:
            last_log = LogfileInfo(filename=filename, date=file_date)

    return last_log


def parse_line(line, format):
    parsed = {}
    end = 0
    for field_name in format.split():
        if field_name.startswith('"') and field_name.endswith('"'):
            m = QUOTED_PATTERN.match(line, end)
        elif field_name.startswith('[') and field_name.endswith(']'):
            m = SQUARE_PATTERN.match(line, end)
        else:
            m = STRING_PATTERN.match(line, end)

        field_name = field_name.strip('"[]')

        try:
            if field_name == 'request':
                request = m.group().split()
                if len(request) < 2:
                    logging.debug('Can\'t parse line: {}'.format(line))
                    return None
                else:
                    parsed['url'] = request[1]

            if field_name == 'request_time':
                parsed[field_name] = float(m.group())
            else:
                parsed[field_name] = m.group()
        except AttributeError:
            return None

        if field_name != format.split()[-1]:
            msep = SEPARATOR_PATTERN.match(line, m.end())
            end = msep.end()

    return parsed


def parse_file(logfile_path, format, errors_limit=None):
    open_func = gzip.open if logfile_path.endswith('.gz') else open
    errors = 0
    records_count = 0

    with open_func(logfile_path, 'rb') as log:
        for line in log:
            records_count += 1
            log_record = parse_line(line, format)
            if not log_record:
                errors += 1
                continue

            yield log_record

    if errors_limit is not None and records_count > 0 and errors / float(records_count) > errors_limit:
        raise RuntimeError('Errors limit exceeded.')


def median(numbers):
    quotient, remainder = divmod(len(numbers), 2)
    if remainder:
        return sorted(numbers)[quotient]

    return sum(numbers[quotient - 1:quotient + 1]) / 2


def calc_stats(log_records):
    stats = {'urls': {}, 'requests_count': 0, 'requests_time_sum': 0}
    for log_record in log_records:
        stats['requests_count'] += 1
        stats['requests_time_sum'] += log_record['request_time']

        url = log_record['url']
        if url not in stats['urls']:
            stats['urls'][url] = {'count': 1,
                                  'url': url,
                                  'requests_times': [log_record['request_time']]}
        else:
            stats['urls'][url]['count'] += 1
            stats['urls'][url]['requests_times'].append(log_record['request_time'])

    for url_record in stats['urls'].values():
        url_record['count_perc'] = url_record['count'] * 100 / stats['requests_count']
        url_record['time_sum'] = sum(url_record['requests_times'])
        url_record['time_perc'] = url_record['time_sum'] * 100 / stats['requests_time_sum']
        url_record['time_avg'] = url_record['time_sum'] / url_record['count']
        url_record['time_max'] = max(url_record['requests_times'])
        url_record['time_med'] = median(url_record['requests_times'])
        del url_record['requests_times']

    return [url for url in stats['urls'].values()]


def make_report(filename, stats, report_size):
    data_for_report = sorted(stats, key=lambda rec: rec['time_sum'], reverse=True)
    json_for_report = json.dumps(data_for_report[:report_size])

    with open('report.html') as template_file:
        template = Template(template_file.read())
        html_report = template.safe_substitute(table_json=json_for_report)

    with open(filename, 'w') as report_file:
        report_file.write(html_report)


def main(config):
    last_log_file = get_last_log_file(config['LOG_DIR'])

    if not last_log_file:
        logging.error("Can't find log files.")
        return

    logging.info('Start parsing {}'.format(last_log_file))
    log_records = parse_file(os.path.join(config['LOG_DIR'], last_log_file.filename),
                             LOG_FORMAT,
                             config['ERRORS_LIMIT'])

    stats = calc_stats(log_records)

    report_filename = 'report-{}.html'.format(last_log_file.date.strftime("%Y.%m.%d"))
    report_filepath = os.path.join(config['REPORT_DIR'], report_filename)

    if os.path.isfile(report_filepath):
        logging.info('Report file already exist.')
        return

    make_report(report_filepath, stats, config['REPORT_SIZE'])


def load_config(config_file=None):
    config = {
        "REPORT_SIZE": 1000,
        "REPORT_DIR": "./reports",
        "LOG_DIR": "./log",
        "ERRORS_LIMIT": 0.05
    }

    if config_file:
        with open(config_file, 'rb') as ext_config_file:
            ext_config = json.load(ext_config_file, encoding='utf8')
        config.update(ext_config)

    return config


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='Config file path')
    args = parser.parse_args()

    config = load_config(args.config)

    logging.basicConfig(filename=config.get('LOG_FILE', None),
                        format='[%(asctime)s] %(levelname).1s %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S',
                        level=config.get('LOG_LEVEL', logging.INFO))

    try:
        main(config)
    except Exception as e:
        logging.exception(e)
