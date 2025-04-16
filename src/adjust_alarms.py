"""
Alarm Adjustment Tool

This tool provides a command-line interface for manually adjusting alarm timeoff values.
It allows users to add, update, list, and remove manual adjustments for alarms that
have no timeoff values in the database.
"""

import os
import json
import argparse
from datetime import datetime
from rich.console import Console
from rich.table import Table

# Import centralized logging
from . import logger_config

# Get a logger for this module
logger = logger_config.get_logger(__name__)
console = Console()

# Path to the manual adjustments file
ADJUSTMENTS_FILE = "./config/manual_adjustments.json"


def load_adjustments():
    """Load the manual adjustments from the JSON file."""
    if not os.path.exists(ADJUSTMENTS_FILE):
        # Create the file with an empty adjustments list if it doesn't exist
        with open(ADJUSTMENTS_FILE, "w") as f:
            json.dump({"adjustments": []}, f, indent=4)
        return {"adjustments": []}

    try:
        with open(ADJUSTMENTS_FILE, "r") as f:
            return json.load(f)
    except json.JSONDecodeError:
        logger.error(f"Error parsing {ADJUSTMENTS_FILE}. File may be corrupted.")
        return {"adjustments": []}


def save_adjustments(adjustments):
    """Save the manual adjustments to the JSON file."""
    try:
        with open(ADJUSTMENTS_FILE, "w") as f:
            json.dump(adjustments, f, indent=4)
        logger.info(f"Adjustments saved to {ADJUSTMENTS_FILE}")
        return True
    except Exception as e:
        logger.error(f"Error saving adjustments: {e}")
        return False


def list_adjustments():
    """List all manual adjustments."""
    adjustments = load_adjustments()

    if not adjustments["adjustments"]:
        console.print("[yellow]No manual adjustments found.[/yellow]")
        return

    table = Table(title="Manual Alarm Adjustments")
    table.add_column("ID", justify="right", style="cyan")
    table.add_column("Alarm Code", justify="right", style="green")
    table.add_column("Station Nr", justify="right", style="green")
    table.add_column("Time On", style="magenta")
    table.add_column("Time Off", style="magenta")
    table.add_column("Notes", style="yellow")
    table.add_column("Last Updated", style="blue")

    for adj in adjustments["adjustments"]:
        table.add_row(
            str(adj["id"]),
            str(adj["alarm_code"]),
            str(adj["station_nr"]),
            adj["time_on"],
            adj["time_off"],
            adj.get("notes", ""),
            adj.get("last_updated", ""),
        )

    console.print(table)


def add_adjustment(args):
    """Add a new manual adjustment."""
    adjustments = load_adjustments()

    # Check if an adjustment with this ID already exists
    for adj in adjustments["adjustments"]:
        if adj["id"] == args.id:
            logger.error(
                f"Adjustment with ID {args.id} already exists. Use update instead."
            )
            return False

    # Validate time format
    try:
        time_on = datetime.strptime(args.time_on, "%Y-%m-%d %H:%M:%S")
        time_off = datetime.strptime(args.time_off, "%Y-%m-%d %H:%M:%S")

        # Ensure time_off is after time_on
        if time_off <= time_on:
            logger.error("Time Off must be after Time On")
            return False
    except ValueError:
        logger.error("Invalid time format. Use YYYY-MM-DD HH:MM:SS")
        return False

    # Create new adjustment
    new_adjustment = {
        "id": args.id,
        "alarm_code": args.alarm_code,
        "station_nr": args.station_nr,
        "time_on": args.time_on,
        "time_off": args.time_off,
        "notes": args.notes if args.notes else "",
        "last_updated": datetime.now().isoformat(),
    }

    adjustments["adjustments"].append(new_adjustment)

    if save_adjustments(adjustments):
        logger.info(f"Added adjustment for alarm ID {args.id}")
        return True
    return False


def update_adjustment(args):
    """Update an existing manual adjustment."""
    adjustments = load_adjustments()

    # Find the adjustment to update
    found = False
    for i, adj in enumerate(adjustments["adjustments"]):
        if adj["id"] == args.id:
            found = True

            # Validate time format if provided
            if args.time_off:
                try:
                    time_on = datetime.strptime(adj["time_on"], "%Y-%m-%d %H:%M:%S")
                    time_off = datetime.strptime(args.time_off, "%Y-%m-%d %H:%M:%S")

                    # Ensure time_off is after time_on
                    if time_off <= time_on:
                        logger.error("Time Off must be after Time On")
                        return False
                except ValueError:
                    logger.error("Invalid time format. Use YYYY-MM-DD HH:MM:SS")
                    return False

                adjustments["adjustments"][i]["time_off"] = args.time_off

            # Update notes if provided
            if args.notes is not None:  # Allow empty string to clear notes
                adjustments["adjustments"][i]["notes"] = args.notes

            adjustments["adjustments"][i]["last_updated"] = datetime.now().isoformat()
            break

    if not found:
        logger.error(f"No adjustment found with ID {args.id}")
        return False

    if save_adjustments(adjustments):
        logger.info(f"Updated adjustment for alarm ID {args.id}")
        return True
    return False


def remove_adjustment(args):
    """Remove a manual adjustment."""
    adjustments = load_adjustments()

    # Find the adjustment to remove
    initial_count = len(adjustments["adjustments"])
    adjustments["adjustments"] = [
        adj for adj in adjustments["adjustments"] if adj["id"] != args.id
    ]

    if len(adjustments["adjustments"]) == initial_count:
        logger.error(f"No adjustment found with ID {args.id}")
        return False

    if save_adjustments(adjustments):
        logger.info(f"Removed adjustment for alarm ID {args.id}")
        return True
    return False


def main():
    parser = argparse.ArgumentParser(description="Manually adjust alarm timeoff values")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # List command
    list_parser = subparsers.add_parser("list", help="List all manual adjustments")

    # Add command
    add_parser = subparsers.add_parser("add", help="Add a new manual adjustment")
    add_parser.add_argument("id", type=int, help="Alarm ID")
    add_parser.add_argument("alarm_code", type=int, help="Alarm code")
    add_parser.add_argument("station_nr", type=int, help="Station number")
    add_parser.add_argument("time_on", help="Time On (YYYY-MM-DD HH:MM:SS)")
    add_parser.add_argument("time_off", help="Time Off (YYYY-MM-DD HH:MM:SS)")
    add_parser.add_argument("--notes", help="Optional notes about this adjustment")

    # Update command
    update_parser = subparsers.add_parser(
        "update", help="Update an existing manual adjustment"
    )
    update_parser.add_argument("id", type=int, help="Alarm ID to update")
    update_parser.add_argument("--time_off", help="New Time Off (YYYY-MM-DD HH:MM:SS)")
    update_parser.add_argument("--notes", help="New notes about this adjustment")

    # Remove command
    remove_parser = subparsers.add_parser("remove", help="Remove a manual adjustment")
    remove_parser.add_argument("id", type=int, help="Alarm ID to remove")

    args = parser.parse_args()

    if args.command == "list" or not args.command:
        list_adjustments()
    elif args.command == "add":
        add_adjustment(args)
    elif args.command == "update":
        update_adjustment(args)
    elif args.command == "remove":
        remove_adjustment(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
