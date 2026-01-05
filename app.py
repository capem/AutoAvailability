"""
Clean Terminal Interface for Wind Farm Data Processing

A minimal, clean interface that suppresses logging output during processing
to avoid display artifacts and provide a smooth user experience.
"""

import os
import sys
import time
import logging
import contextlib
import argparse
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from io import StringIO

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm
from rich.text import Text

from src import config
from src import logger_config
from src import data_exporter
from src import calculation
from src import hebdo_calc
from src import email_send
from src import adjust_alarms

# Get a logger for this module
logger = logger_config.get_logger(__name__)

console = Console()


@contextlib.contextmanager
def suppress_logging():
    """Context manager to suppress console logging output while preserving file logging"""
    # Get the root logger
    root_logger = logging.getLogger()
    
    # Find the console handler (RichHandler) and temporarily remove it
    console_handlers = [handler for handler in root_logger.handlers if isinstance(handler, logger_config.RichHandler)]
    
    try:
        # Remove console handlers temporarily
        for handler in console_handlers:
            root_logger.removeHandler(handler)
        
        # Keep file logging active
        yield
    finally:
        # Restore console handlers
        for handler in console_handlers:
            root_logger.addHandler(handler)


class CleanWindFarmTUI:
    """Clean TUI for Wind Farm Data Processing"""
    
    def __init__(self):
        self.running = True
        self.last_run_date = None
        self.email_enabled = True
        self.update_mode = "append"
        
    def run(self):
        """Main TUI loop"""
        console.clear()
        self.show_header()
        
        while self.running:
            try:
                self.show_main_menu()
                choice = Prompt.ask(
                    "Select an option",
                    choices=["1", "2", "3", "4", "5", "6", "7", "8", "q"],
                    default="q"
                )
                
                if choice == "1":
                    self.run_today()
                elif choice == "2":
                    self.run_yesterday()
                elif choice == "3":
                    self.run_custom_date()
                elif choice == "4":
                    self.manage_alarms()
                elif choice == "5":
                    self.view_logs()
                elif choice == "6":
                    self.settings()
                elif choice == "7":
                    self.system_status()
                elif choice == "8":
                    self.help()
                elif choice == "q":
                    self.quit()
                    
            except KeyboardInterrupt:
                console.print("\n[yellow]Operation cancelled by user[/yellow]")
                if Confirm.ask("Do you want to quit?"):
                    self.quit()
            except Exception as e:
                console.print(f"[red]Error: {str(e)}[/red]")
                console.print("Press Enter to continue...")
                input()
                
    def show_header(self):
        """Display application header"""
        header = Panel.fit(
            "[bold blue]Wind Farm Data Processing System[/bold blue]\n"
            "[dim]Clean Terminal Interface[/dim]",
            border_style="blue"
        )
        console.print(header)
        console.print()
        
    def show_main_menu(self):
        """Display main menu"""
        menu = Table(show_header=False, box=None, padding=(0, 2))
        menu.add_column("Option", style="cyan", width=8)
        menu.add_column("Description", style="white")
        
        menu.add_row("1", "Run Today - Process data for today")
        menu.add_row("2", "Run Yesterday - Process data for yesterday")
        menu.add_row("3", "Custom Date - Process data for specific date(s)")
        menu.add_row("4", "Manage Alarms - Manual alarm adjustments")
        menu.add_row("5", "View Logs - Application logs")
        menu.add_row("6", "Settings - Application configuration")
        menu.add_row("7", "System Status - Check system health")
        menu.add_row("8", "Help - Show help information")
        menu.add_row("q", "Quit - Exit application")
        
        console.print(Panel(menu, title="Main Menu", border_style="green"))
        console.print()
        
    def run_today(self):
        """Process data for today"""
        target_date = datetime.now()
        self.process_dates([target_date])
        
    def run_yesterday(self):
        """Process data for yesterday"""
        target_date = datetime.now() - timedelta(days=1)
        self.process_dates([target_date])
        
    def run_custom_date(self):
        """Process data for custom date(s)"""
        console.print("[bold]Custom Date Processing[/bold]")
        console.print()
        
        date_input = Prompt.ask(
            "Enter date(s) (YYYY-MM-DD, comma-separated for multiple)",
            default=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        )
        
        # Parse dates
        dates = []
        try:
            for date_str in date_input.split(","):
                dates.append(datetime.strptime(date_str.strip(), "%Y-%m-%d"))
        except ValueError:
            console.print("[red]Invalid date format. Please use YYYY-MM-DD[/red]")
            return
            
        # Get update mode
        console.print("\nUpdate modes:")
        console.print("1. Append (default) - Update/append while preserving deleted records")
        console.print("2. Check - Report changes without modifying existing data")
        console.print("3. Force overwrite - Export fresh data, overwriting existing files")
        console.print("4. Process existing - Skip DB/export, process existing files only")
        
        mode_choice = Prompt.ask(
            "Select update mode",
            choices=["1", "2", "3", "4"],
            default="1"
        )
        
        mode_map = {
            "1": "append",
            "2": "check", 
            "3": "force-overwrite",
            "4": "process-existing"
        }
        self.update_mode = mode_map[mode_choice]
        
        # Process all dates
        self.process_dates(dates)

    def process_dates(self, dates: List[datetime]):
        """Process a list of dates and show a single confirmation at the end."""
        for date in dates:
            self.process_date(date)

        console.print("\nPress Enter to continue...")
        input()
            
    def process_date(self, target_date: datetime):
        """Process data for a specific date with clean output"""
        console.print(f"\n[bold]Processing data for {target_date.strftime('%Y-%m-%d')}[/bold]")
        console.print(f"Update mode: {self.update_mode}")
        console.print()
        
        try:
            # Step 1: Data Export
            console.print("[cyan]Step 1/4: Exporting data from database...[/cyan]")
            with suppress_logging():
                self._export_data_clean(target_date)
            console.print("[green]✓ Data export completed[/green]")
            
            # Step 2: Calculations
            console.print("[cyan]Step 2/4: Running availability calculations...[/cyan]")
            with suppress_logging():
                self._run_calculations_clean(target_date)
            console.print("[green]✓ Calculations completed[/green]")
            
            # Step 3: Weekly calculations
            console.print("[cyan]Step 3/4: Generating weekly reports...[/cyan]")
            with suppress_logging():
                results = self._run_weekly_calculations_clean(target_date)
            console.print("[green]✓ Weekly calculations completed[/green]")
            
            # Step 4: Email reports
            if self.email_enabled:
                console.print("[cyan]Step 4/4: Sending email reports...[/cyan]")
                with suppress_logging():
                    self._send_email_reports_clean(target_date, results)
                console.print("[green]✓ Email reports sent[/green]")
            else:
                console.print("[yellow]Step 4/4: Email reports skipped (disabled)[/yellow]")
                    
            
            console.print(f"\n[bold green]✓ Processing completed successfully for {target_date.strftime('%Y-%m-%d')}[/bold green]")
            self.last_run_date = target_date
            
        except Exception as e:
            console.print(f"\n[bold red]✗ Processing failed: {str(e)}[/bold red]")
            logger.exception("[APP] Processing failed")
        
    def _export_data_clean(self, run_date: datetime):
        """Clean data export"""
        period_start_dt = run_date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=6)
        period_end_dt = run_date.replace(hour=23, minute=50, second=0, microsecond=0)
        
        import pandas as pd
        period_range = pd.period_range(start=period_start_dt, end=period_end_dt, freq="M")
        
        for period in period_range:
            period_str = period.strftime("%Y-%m")
            data_exporter.main_export_flow(period=period_str, update_mode=self.update_mode)
            
    def _run_calculations_clean(self, run_date: datetime):
        """Clean calculations"""
        period_start_dt = run_date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=6)
        period_end_dt = run_date.replace(hour=23, minute=50, second=0, microsecond=0)
        
        import pandas as pd
        period_range = pd.period_range(start=period_start_dt, end=period_end_dt, freq="M")
        
        for period in period_range:
            period_month = period.strftime("%Y-%m")
            results = calculation.full_calculation(period_month)
            results.to_pickle(f"./monthly_data/results/{period_month}.pkl")
            
            from src import results_grouper
            results_grouper.process_grouped_results(results, period_month)
            
    def _run_weekly_calculations_clean(self, run_date: datetime):
        """Clean weekly calculations"""
        period_start_dt = run_date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=6)
        period_end_dt = run_date.replace(hour=23, minute=50, second=0, microsecond=0)
        
        import pandas as pd
        period_range = pd.period_range(start=period_start_dt, end=period_end_dt, freq="M")
        
        df_exploi = hebdo_calc.main(period_range, period_start_dt, period_end_dt)
        df_Top15 = hebdo_calc.Top15(period_range, period_start_dt, period_end_dt)
        
        return {
            'df_exploi': df_exploi,
            'df_Top15': df_Top15,
            'period_start': period_start_dt,
            'period_end': period_end_dt
        }
        
    def _send_email_reports_clean(self, run_date: datetime, results: dict):
        """Clean email sending"""
        title = f"From {results['period_start'].strftime('%Y_%m_%d')} To {results['period_end'].strftime('%Y_%m_%d')}"
        
        email_send.send_email(
            df=results['df_exploi'],
            receiver_email=config.EMAIL_CONFIG["receiver_default"],
            subject=f"Indisponibilité {title}"
        )
        
        email_send.send_email(
            df=results['df_Top15'],
            receiver_email=config.EMAIL_CONFIG["receiver_default"],
            subject=f"Top 15 Total Energy Lost(MWh){title}"
        )

    def manage_alarms(self):
        """Manage manual alarm adjustments"""
        console.print("[bold]Manual Alarm Adjustments[/bold]")
        console.print()

        while True:
            console.print("Alarm Management:")
            console.print("1. List all adjustments")
            console.print("2. Add new adjustment")
            console.print("3. Edit adjustment")
            console.print("4. Delete adjustment")
            console.print("5. Back to main menu")

            choice = Prompt.ask("Select option", choices=["1", "2", "3", "4", "5"], default="5")

            if choice == "1":
                self._list_adjustments()
            elif choice == "2":
                self._add_adjustment()
            elif choice == "3":
                self._edit_adjustment()
            elif choice == "4":
                self._delete_adjustment()
            elif choice == "5":
                break

    def _list_adjustments(self):
        """List all alarm adjustments"""
        adjustments = adjust_alarms.load_adjustments()

        if not adjustments.get("adjustments"):
            console.print("[yellow]No manual adjustments found[/yellow]")
            return

        table = Table(title="Manual Alarm Adjustments")
        table.add_column("ID", justify="right", style="cyan")
        table.add_column("Alarm Code", justify="right", style="green")
        table.add_column("Station Nr", justify="right", style="green")
        table.add_column("Time On", style="white")
        table.add_column("Time Off", style="white")
        table.add_column("Notes", style="yellow")
        table.add_column("Last Updated", style="blue")

        for adj in adjustments["adjustments"]:
            last_updated_str = adj.get("last_updated", "")
            if last_updated_str:
                try:
                    # Parse ISO format and reformat for display
                    last_updated_dt = datetime.fromisoformat(last_updated_str)
                    last_updated_str = last_updated_dt.strftime("%Y-%m-%d %H:%M:%S")
                except (ValueError, TypeError):
                    # If parsing fails, show the raw value
                    pass
            
            table.add_row(
                str(adj.get("id", "N/A")),
                str(adj.get("alarm_code", "N/A")),
                str(adj.get("station_nr", "N/A")),
                adj.get("time_on", "N/A"),
                adj.get("time_off", "N/A"),
                adj.get("notes", ""),
                last_updated_str,
            )

        console.print(table)
        console.print("\nPress Enter to continue...")
        input()

    def _add_adjustment(self):
        """Add new alarm adjustment"""
        console.print("[bold]Add New Alarm Adjustment[/bold]")

        try:
            alarm_id = int(Prompt.ask("Alarm ID"))
            alarm_code = int(Prompt.ask("Alarm Code"))
            station_nr = int(Prompt.ask("Station Number"))
            time_on = Prompt.ask("Time On (YYYY-MM-DD HH:MM:SS, or Enter to skip)", default="")
            time_off = Prompt.ask("Time Off (YYYY-MM-DD HH:MM:SS, or Enter to skip)", default="")
            notes = Prompt.ask("Notes (optional)", default="")

            if not time_on and not time_off:
                raise ValueError("At least one of Time On or Time Off must be provided.")

            # Validate time format
            if time_on:
                datetime.strptime(time_on, "%Y-%m-%d %H:%M:%S")
            if time_off:
                datetime.strptime(time_off, "%Y-%m-%d %H:%M:%S")

            # Create mock args object for adjust_alarms functions
            class MockArgs:
                def __init__(self):
                    self.id = alarm_id
                    self.alarm_code = alarm_code
                    self.station_nr = station_nr
                    self.time_on = time_on
                    self.time_off = time_off
                    self.notes = notes

            if adjust_alarms.add_adjustment(MockArgs()):
                console.print("[green]✓ Adjustment added successfully[/green]")
            else:
                console.print("[red]✗ Failed to add adjustment[/red]")

        except ValueError as e:
            console.print(f"[red]Invalid input: {str(e)}[/red]")
        except Exception as e:
            console.print(f"[red]Error: {str(e)}[/red]")

        console.print("\nPress Enter to continue...")
        input()

    def _edit_adjustment(self):
        """Edit existing alarm adjustment"""
        console.print("[bold]Edit Alarm Adjustment[/bold]")

        try:
            alarm_id = int(Prompt.ask("Enter Alarm ID to edit"))

            # Load current adjustments to check if ID exists
            adjustments = adjust_alarms.load_adjustments()
            found = False
            for adj in adjustments.get("adjustments", []):
                if adj["id"] == alarm_id:
                    found = True
                    console.print(f"Current values for Alarm ID {alarm_id}:")
                    console.print(f"  Alarm Code: {adj['alarm_code']}")
                    console.print(f"  Station Nr: {adj.get('station_nr', 'N/A')}")
                    console.print(f"  Time On: {adj.get('time_on', 'N/A')}")
                    console.print(f"  Time Off: {adj.get('time_off', 'N/A')}")
                    console.print(f"  Notes: {adj.get('notes', '')}")
                    break

            if not found:
                console.print(f"[red]No adjustment found with ID {alarm_id}[/red]")
                return

            # Get new values (optional)
            time_on = Prompt.ask("New Time On (YYYY-MM-DD HH:MM:SS, or Enter to keep current)", default="")
            time_off = Prompt.ask("New Time Off (YYYY-MM-DD HH:MM:SS, or Enter to keep current)", default="")
            notes = Prompt.ask("New Notes (or Enter to keep current)", default="")

            # Create mock args object
            class MockArgs:
                def __init__(self):
                    self.id = alarm_id
                    self.time_on = time_on if time_on else None
                    self.time_off = time_off if time_off else None
                    self.notes = notes if notes else None

            if adjust_alarms.update_adjustment(MockArgs()):
                console.print("[green]✓ Adjustment updated successfully[/green]")
            else:
                console.print("[red]✗ Failed to update adjustment[/red]")

        except ValueError as e:
            console.print(f"[red]Invalid input: {str(e)}[/red]")
        except Exception as e:
            console.print(f"[red]Error: {str(e)}[/red]")

        console.print("\nPress Enter to continue...")
        input()

    def _delete_adjustment(self):
        """Delete alarm adjustment"""
        console.print("[bold]Delete Alarm Adjustment[/bold]")

        try:
            alarm_id = int(Prompt.ask("Enter Alarm ID to delete"))

            if Confirm.ask(f"Are you sure you want to delete adjustment for Alarm ID {alarm_id}?"):
                class MockArgs:
                    def __init__(self):
                        self.id = alarm_id

                if adjust_alarms.remove_adjustment(MockArgs()):
                    console.print("[green]✓ Adjustment deleted successfully[/green]")
                else:
                    console.print("[red]✗ Failed to delete adjustment[/red]")
            else:
                console.print("[yellow]Deletion cancelled[/yellow]")

        except ValueError as e:
            console.print(f"[red]Invalid input: {str(e)}[/red]")
        except Exception as e:
            console.print(f"[red]Error: {str(e)}[/red]")

        console.print("\nPress Enter to continue...")
        input()

    def view_logs(self):
        """View application logs"""
        console.print("[bold]Application Logs[/bold]")
        console.print()

        try:
            log_file = "./logs/application.log"
            if os.path.exists(log_file):
                with open(log_file, "r") as f:
                    lines = f.readlines()

                # Show last 50 lines
                recent_lines = lines[-50:] if len(lines) > 50 else lines

                for line in recent_lines:
                    console.print(line.strip())

                console.print(f"\n[dim]Showing last {len(recent_lines)} lines of {len(lines)} total[/dim]")
            else:
                console.print("[yellow]No log file found[/yellow]")

        except Exception as e:
            console.print(f"[red]Error reading logs: {str(e)}[/red]")

        console.print("\nPress Enter to continue...")
        input()

    def settings(self):
        """Application settings"""
        console.print("[bold]Application Settings[/bold]")
        console.print()

        while True:
            console.print(f"Current Settings:")
            console.print(f"  Email Enabled: {'Yes' if self.email_enabled else 'No'}")
            console.print(f"  Update Mode: {self.update_mode}")
            console.print(f"  Last Run: {self.last_run_date.strftime('%Y-%m-%d %H:%M:%S') if self.last_run_date else 'Never'}")
            console.print()

            console.print("Settings Menu:")
            console.print("1. Toggle email notifications")
            console.print("2. Change default update mode")
            console.print("3. Test database connection")
            console.print("4. Test email configuration")
            console.print("5. Back to main menu")

            choice = Prompt.ask("Select option", choices=["1", "2", "3", "4", "5"], default="5")

            if choice == "1":
                self.email_enabled = not self.email_enabled
                status = "enabled" if self.email_enabled else "disabled"
                console.print(f"[green]Email notifications {status}[/green]")
            elif choice == "2":
                self._change_update_mode()
            elif choice == "3":
                self._test_database_connection()
            elif choice == "4":
                self._test_email_configuration()
            elif choice == "5":
                break

    def _change_update_mode(self):
        """Change default update mode"""
        console.print("Update modes:")
        console.print("1. Append - Update/append while preserving deleted records")
        console.print("2. Check - Report changes without modifying existing data")
        console.print("3. Force overwrite - Export fresh data, overwriting existing files")
        console.print("4. Process existing - Skip DB/export, process existing files only")

        choice = Prompt.ask("Select new default mode", choices=["1", "2", "3", "4"])

        mode_map = {"1": "append", "2": "check", "3": "force-overwrite", "4": "process-existing"}
        self.update_mode = mode_map[choice]

        console.print(f"[green]Default update mode changed to: {self.update_mode}[/green]")

    def _test_database_connection(self):
        """Test database connection"""
        console.print("Testing database connection...")

        try:
            # Simple test - try to import and check config
            db_config = config.DB_CONFIG
            console.print(f"[green]✓ Database configuration loaded[/green]")
            console.print(f"  Server: {db_config['server']}")
            console.print(f"  Database: {db_config['database']}")
            console.print(f"  Username: {db_config['username']}")

        except Exception as e:
            console.print(f"[red]✗ Database configuration error: {str(e)}[/red]")

    def _test_email_configuration(self):
        """Test email configuration"""
        console.print("Testing email configuration...")

        try:
            email_config = config.EMAIL_CONFIG
            console.print(f"[green]✓ Email configuration loaded[/green]")
            console.print(f"  Sender: {email_config['sender_email']}")
            console.print(f"  SMTP Host: {email_config['smtp_host']}")
            console.print(f"  SMTP Port: {email_config['smtp_port']}")
            console.print(f"  Default Recipient: {email_config['receiver_default']}")

        except Exception as e:
            console.print(f"[red]✗ Email configuration error: {str(e)}[/red]")

    def system_status(self):
        """Show system status"""
        console.print("[bold]System Status[/bold]")
        console.print()

        status_table = Table(title="System Health Check")
        status_table.add_column("Component", style="cyan")
        status_table.add_column("Status", style="white")
        status_table.add_column("Details", style="dim")

        # Check configuration
        try:
            config.DB_CONFIG
            status_table.add_row("Database Config", "[green]✓ OK[/green]", "Configuration loaded")
        except Exception as e:
            status_table.add_row("Database Config", "[red]✗ Error[/red]", str(e))

        # Check email config
        try:
            config.EMAIL_CONFIG
            status_table.add_row("Email Config", "[green]✓ OK[/green]", "Configuration loaded")
        except Exception as e:
            status_table.add_row("Email Config", "[red]✗ Error[/red]", str(e))

        # Check data directories
        data_path = config.BASE_DATA_PATH
        if os.path.exists(data_path):
            status_table.add_row("Data Directory", "[green]✓ OK[/green]", f"Path: {data_path}")
        else:
            status_table.add_row("Data Directory", "[yellow]⚠ Missing[/yellow]", f"Path: {data_path}")

        # Check log directory
        log_path = "./logs"
        if os.path.exists(log_path):
            status_table.add_row("Log Directory", "[green]✓ OK[/green]", f"Path: {log_path}")
        else:
            status_table.add_row("Log Directory", "[yellow]⚠ Missing[/yellow]", f"Path: {log_path}")

        console.print(status_table)
        console.print("\nPress Enter to continue...")
        input()

    def help(self):
        """Show help information"""
        help_text = """
[bold]Wind Farm Data Processing System - Help[/bold]

[cyan]Main Functions:[/cyan]
• Run Today/Yesterday: Process wind farm data for specific dates
• Custom Date: Process data for one or more custom dates
• Manage Alarms: Add, edit, or delete manual alarm adjustments
• View Logs: Check application logs for troubleshooting
• Settings: Configure email notifications and update modes
• System Status: Check system health and configuration

[cyan]Update Modes:[/cyan]
• Append: Update existing data while preserving deleted records (recommended)
• Check: Report what would change without making modifications
• Force Overwrite: Export fresh data, replacing existing files
• Process Existing: Skip database export, only process existing files

[cyan]Processing Steps:[/cyan]
1. Export data from SQL Server database
2. Run availability calculations
3. Generate weekly reports
4. Send email notifications (if enabled)

This clean interface suppresses logging output during processing for a smooth experience.
For detailed logs, use option 5 (View Logs) after processing.
        """

        console.print(Panel(help_text, border_style="blue"))
        console.print("\nPress Enter to continue...")
        input()

    def quit(self):
        """Quit the application"""
        console.print("[yellow]Shutting down Wind Farm TUI...[/yellow]")
        self.running = False






def main():
    """Main entry point for clean TUI"""
    parser = argparse.ArgumentParser(description="Wind Farm Data Processing System")
    args = parser.parse_args()

    # Initialize logging
    logger_config.configure_logging()

    # Run the TUI
    try:
        tui = CleanWindFarmTUI()
        tui.run()
    except Exception as e:
        console.print(f"[red]Fatal error: {str(e)}[/red]")
        logger.exception("[APP] Fatal error in TUI")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())