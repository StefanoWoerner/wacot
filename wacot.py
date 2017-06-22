#!/usr/bin/env python3

import argparse
import Importer
import Analyzer
import Processor
import Exporter
import create_db


def main():
    # create the top-level parser
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(
        title='possible subcommands', dest='command')
    subparsers.required = True

    # create the parser for the create command
    parser_create = subparsers.add_parser(
        'create', help='Creates the database for wacot.')
    parser_create.set_defaults(func=wacot_create)

    # create the parser for the import command
    parser_import = subparsers.add_parser(
        'import', help='Imports the data from the wikipedia dump.')
    parser_import.add_argument(
        '--from-dumps', choices=['xml', 'cat', 'all'],
        help=('Select from which dump files to import: '
              'xml imports only from the XML dump, '
              'cat imports only from the category and categorylink SQL files '
              'and all imports from both. Default is all.'))
    parser_import.add_argument(
        '--only-import', action="store_true",
        help=('Only imports the data but does not analyze it. '
              'By default the data is analyzed after the import.'))
    parser_import.set_defaults(func=wacot_import, from_dumps='all')

    # create the parser for the analyze command
    parser_analyze = subparsers.add_parser(
        'analyze', help=('Analyzes the data and computes the '
                         'contribution tables, bot flags, '
                         'contribution counts and edit_counts.'))
    parser_analyze.set_defaults(func=wacot_analyze)

    # create the parser for the process command
    parser_process = subparsers.add_parser(
        'process', help='Processes the data to generate co-authorship tables.')
    parser_process.add_argument(
        'object', choices=['article-similarities', 'category-similarities'],
        help=('Select if you want to compute co-authorship '
              'for articles or categories'))
    parser_process.set_defaults(func=wacot_process)

    # create the parser for the export command
    parser_export = subparsers.add_parser(
        'export', help='Creates the database for wacot.')
    parser_export.add_argument(
        'object', choices=['article-similarities', 'category-similarities'],
        help=('Select if you want to export similarities '
              'for articles or categories'))
    parser_export.add_argument(
        '--format', choices=['graphml', 'csv'],
        help='Select export format. Default is graphml.')
    parser_export.set_defaults(func=wacot_export, format='graphml')

    # parse the args and call the function for the selected command
    args = parser.parse_args()
    args.func(args)


def wacot_create(args):
    create_db.main()


def wacot_import(args):
    importer = Importer.Importer()
    if args.from_dumps == 'all' or args.from_dumps == 'xml':
        importer.import_xml()
    if args.from_dumps == 'all' or args.from_dumps == 'cat':
        importer.import_categories()
    if not args.only_import:
        analyzer = Analyzer.Analyzer()
        analyzer.compute_article_contributions()
        analyzer.compute_category_contributions()
        analyzer.compute_bot_flags()
        analyzer.count_article_contributions()
        analyzer.count_category_contributions()


def wacot_analyze(args):
    analyzer = Analyzer.Analyzer()
    analyzer.compute_article_contributions()
    analyzer.compute_category_contributions()
    analyzer.compute_bot_flags()
    analyzer.count_article_contributions()
    analyzer.count_category_contributions()


def wacot_process(args):
    processor = Processor.Processor()
    if args.object == 'article-similarities':
        processor.generate_article_co_authorship()
    elif args.object == 'category-similarities':
        processor.generate_category_co_authorship()


def wacot_export(args):
    exporter = Exporter.Exporter()
    if args.object == 'article-similarities':
        if args.format == 'graphml':
            exporter.export_article_similarities_to_graphml()
        elif args.format == 'csv':
            exporter.export_article_similarities_to_graphml()
    elif args.object == 'category-similarities':
        if args.format == 'graphml':
            exporter.export_category_similarities_to_graphml()
        elif args.format == 'csv':
            exporter.export_category_similarities_to_graphml()


if (__name__ == "__main__"):
    main()
