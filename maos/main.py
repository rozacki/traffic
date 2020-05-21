import sys
from argparse import *
from maos.common import valid_date
from maos import pipeline


class MaosArgsParser:
    def __init__(self):
        parser = ArgumentParser(description='maos tool', usage='maos command arguments...')
        parser.add_argument('command', help='subcommand to run', choices=['download', 'ingest'])
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print('unrecognized command')
            parser.print_help()
            exit(1)
        getattr(self, args.command)()

    def download(self):
        parser = ArgumentParser(description='choose what entity to download')
        parser.add_argument('entity', type=str, help='entity to download',
                            choices=['site', 'link', 'road'])
        args = parser.parse_args(sys.argv[2:3])
        download_entity_command = 'download_' + args.entity
        if not hasattr(self, download_entity_command):
            print(f'not supported entity "{args.entity}"')
            parser.print_help()
            exit(1)
        getattr(self, download_entity_command)()


    @staticmethod
    def _prepare_parser():
        parser = ArgumentParser(description='download sites for period of time')
        parser.add_argument('--sites-catalog', type=str, default='sites_catalog.csv')
        parser.add_argument('-s', '--startdate', help='The Start Date - format YYYY-MM-DD', type=valid_date,
                            required=True)
        parser.add_argument('-e', '--enddate', help='The Stop Date - format YYYY-MM-DD', type=valid_date,
                            required=True)
        return parser

    @staticmethod
    def download_road():
        parser = MaosArgsParser._prepare_parser()
        parser.add_argument('--road', type=str, required=True,
                            help='If entity is road, Seek and downloading all sites on the road related')
        args = parser.parse_args(sys.argv[3:])
        pipeline.download_road_reports(args.road, args.startdate, args.enddate)

    @staticmethod
    def download_site():
        parser = ArgumentParser(description='download sites for period of time')
        parser.add_argument('--site', type=int, default=1, help='If entity is site,'
                                                                ' Seek and start downloading from this site')
        parser.add_argument('--sites-count', type=int, default=1, help='How many sites try to download. '
                                                                       'Note some data may no be available for site',
                            required=True)
        parser.add_argument('--sites-catalog', type=str, default='sites_catalog.csv')
        parser.add_argument('-s', '--startdate', help='The Start Date - format YYYY-MM-DD', type=valid_date,
                            required=True)
        parser.add_argument('-e', '--enddate', help='The Stop Date - format YYYY-MM-DD', type=valid_date,
                            required=True)

        args = parser.parse_args(sys.argv[3:])
        pipeline.download_sites_reports(args.site, args.sites_count, args.startdate, args.enddate)

    @staticmethod
    def download_link():
        parser = ArgumentParser(description='download link for period of time')
        parser.add_argument('-l', '--link', help='Link id, all related sites will be downloaded', required=True)
        parser.add_argument('-s', '--startdate', help='The Start Date - format YYYY-MM-DD', type=valid_date,
                            required=True)
        parser.add_argument('-e', '--enddate', help='The Stop Date - format YYYY-MM-DD', type=valid_date,
                            required=True)

        args = parser.parse_args(sys.argv[3:])
        pipeline.download_link_reports(args.link, args.startdate, args.enddate)

    @staticmethod
    def ingest():
        parser = ArgumentParser(description='ingest reports to druid')
        parser.add_argument('--datasource', help='data source name', required=True)
        parser.add_argument('--source-folder', help='data source name', required=True)
        parser.add_argument('--append-to-existing', help='overwrite existing data source', action='store_true')

        args = parser.parse_args(sys.argv[2:])
        pipeline.ingest(args.datasource, args.source_folder, args.overwrite)


if __name__ == '__main__':
    MaosArgsParser()