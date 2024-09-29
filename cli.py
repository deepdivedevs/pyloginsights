import argparse
from datetime import datetime
from typing import Dict, Any
from pyloginsight import PyLogInsight

class CLIParser:
    def __init__(self):
        self.parser = self._create_parser()
        self.log_insight = PyLogInsight()

    def _create_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description="PyLogInsight: Python Logging and Analysis Tool")
        subparsers = parser.add_subparsers(dest="command", help="Available commands")

        self._add_query_parser(subparsers)
        self._add_export_parser(subparsers)

        return parser

    def _add_query_parser(self, subparsers):
        query_parser = subparsers.add_parser("query", help="Query logs")
        self._add_common_arguments(query_parser)
        query_parser.add_argument("--limit", type=int, default=100, help="Limit the number of results")
        query_parser.add_argument("--sort-by", choices=["timestamp", "execution_time", "avg_memory"],
                                  help="Sort results by this field")
        query_parser.add_argument("--descending", action="store_true", help="Sort in descending order")

    def _add_export_parser(self, subparsers):
        export_parser = subparsers.add_parser("export", help="Export logs")
        self._add_common_arguments(export_parser)
        export_parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Export format")
        export_parser.add_argument("--output", required=True, help="Output file path")

    def _add_common_arguments(self, parser):
        parser.add_argument("--function-name", help="Filter by function name")
        parser.add_argument("--start-date", type=self._parse_date, help="Start date (YYYY-MM-DD)")
        parser.add_argument("--end-date", type=self._parse_date, help="End date (YYYY-MM-DD)")
        parser.add_argument("--min-execution-time", type=float, help="Minimum execution time")
        parser.add_argument("--max-execution-time", type=float, help="Maximum execution time")
        parser.add_argument("--stdout", help="Filter stdout containing this string")

    @staticmethod
    def _parse_date(date_str: str) -> float:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").timestamp()
        except ValueError:
            raise argparse.ArgumentTypeError(f"Invalid date format: {date_str}. Use YYYY-MM-DD.")

    def _build_filters(self, args: argparse.Namespace) -> Dict[str, Any]:
        filters = {}
        if args.function_name:
            filters["function_name"] = args.function_name
        if args.start_date:
            filters.setdefault("timestamp", {})["start"] = args.start_date
        if args.end_date:
            filters.setdefault("timestamp", {})["end"] = args.end_date
        if args.min_execution_time:
            filters.setdefault("execution_time", {})["min"] = args.min_execution_time
        if args.max_execution_time:
            filters.setdefault("execution_time", {})["max"] = args.max_execution_time
        if args.stdout:
            filters["stdout"] = {"contains": args.stdout}
        return filters

    def query(self, args: argparse.Namespace):
        filters = self._build_filters(args)
        sort_by = [(args.sort_by, "desc" if args.descending else "asc")] if args.sort_by else None
        results = self.log_insight.query_logs(filters=filters, sort_by=sort_by)

        for index, row in results.iterrows():
            if index >= args.limit:
                break
            print(f"Log {index + 1}:")
            for column, value in row.items():
                print(f"  {column}: {value}")
            print()

    def export(self, args: argparse.Namespace):
        filters = self._build_filters(args)
        results = self.log_insight.query_logs(filters=filters)
        success = self.log_insight.export_logs(results, args.output, format=args.format)
        if success:
            print(f"Logs exported successfully to {args.output}")
        else:
            print("Failed to export logs")

    def run(self):
        args = self.parser.parse_args()
        if args.command == "query":
            self.query(args)
        elif args.command == "export":
            self.export(args)
        else:
            self.parser.print_help()
