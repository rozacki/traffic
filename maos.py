import sys
from argparse import *
from datetime import datetime
from common import valid_date
import pipeline


class MaosArgsParser():
    def __init__(self):
        parser = ArgumentParser(description='maos tool', usage='maos command arguments')
        parser.add_argument('command', help='subcommand to run')
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print('unrecognized command')
            parser.print_help()
            exit(1)
        getattr(self, args.command)()

    def download(self):
        parser = ArgumentParser(description='download sites for period of time')
        parser.add_argument('--site', type=int, default=1, help='Seek and start downloading from this site')
        parser.add_argument('--sites-count', type=int, default=1, help='How many sites try to download. '
                                                                       'Note some data may no be available for site')
        parser.add_argument('--sites-file', type=str, default='sites_enriched_roads.csv')
        parser.add_argument('-s', '--startdate', help="The Start Date - format YYYY-MM-DD", type=valid_date)
        parser.add_argument('-e', '--enddate', help="The Stop Date - format YYYY-MM-DD", type=valid_date,
                            default=datetime.now().strftime('%Y-%m-%d'))

        args = parser.parse_args(sys.argv[2:])
        pipeline.download_reports_async(args.site, args.sites_count, args.startdate, args.enddate)

    def ingest(self):
        parser = ArgumentParser(description='ingest reports to druid')
        parser.add_argument('--datasource', help='data source name', required=True)
        parser.add_argument('--source_folder', help='data source name', required=True)

        args = parser.parse_args(sys.argv[2:])
        pipeline.ingest(args.datasource, args.source_folder)


if __name__ == '__main__':
    MaosArgsParser()