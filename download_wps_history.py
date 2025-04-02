# working httpx

import os
import httpx
from bs4 import BeautifulSoup
import logging
import pandas as pd
from rich.console import Console
from rich.progress import (
    BarColumn,
    TextColumn,
    DownloadColumn,
    TimeRemainingColumn,
    FileSizeColumn,
    TransferSpeedColumn,
    Progress,
)
from rich.logging import RichHandler
import concurrent.futures


def setup_default_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True)],
    )


logger = logging.getLogger("rich")
IE11_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko"
BASE_FILE_PATH = "./monthly_data/uploads"


class SimpleLoginManager:
    def __init__(self):
        headers = {
            "User-Agent": IE11_USER_AGENT,
            "Accept": "text/html, application/xhtml+xml, image/jxr, */*",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "en-GB, en; q=0.8, fr-FR; q=0.5, fr; q=0.2",
            "Cache-Control": "no-cache",
            "Connection": "Keep-Alive",
            "Content-Type": "application/x-www-form-urlencoded",
            "Host": "10.173.224.100",
            "Referer": "http://10.173.224.100/Login.aspx",
        }
        self.session = httpx.Client(headers=headers, timeout=15)
        self.login_url = "http://10.173.224.100/Login.aspx"
        self.authenticate_url = "http://10.173.224.100/Authenticate.aspx"

    def fetch_login_page(self):
        response = self.session.get(self.login_url)
        return response.text

    def extract_hidden_fields(self, html_content):
        soup = BeautifulSoup(html_content, "html.parser")
        hidden_fields = {}
        for input_tag in soup.find_all("input", attrs={"type": "hidden"}):
            hidden_fields[input_tag["name"]] = input_tag["value"]
        return hidden_fields

    def login(self, username, password):
        login_page_content = self.fetch_login_page()
        hidden_fields = self.extract_hidden_fields(login_page_content)

        login_data = {
            "userName": username,
            "password": password,
            "submit": "Login",
            **hidden_fields,
        }

        response = self.session.post(self.authenticate_url, data=login_data)

        if (
            response.status_code == 302
            and "Location" in response.headers
            and response.headers["Location"] == "/Welcome.aspx"
        ):
            return True
        else:
            logging.error(f"Login failed with response content: {response.text}")
            return False

    def download_file(self, url, filepath, progress, task_id):
        with self.session.stream("GET", url) as response:
            total_length = int(response.headers.get("content-length", 0))
            progress.update(task_id, total=total_length)

            if response.status_code == 200:
                if os.path.exists(filepath):
                    try:
                        expected_file_size = int(self.session.head(url).headers.get("content-length", 0))
                    except:
                        logger.error(f"Failed to get content length for {url}. Skipping download...")
                        return

                    if os.path.getsize(filepath) == expected_file_size:
                        logger.info(f"File {filepath} already downloaded. Skipping...")
                        progress.update(task_id, completed=expected_file_size)
                        return

                with open(filepath, "wb") as file:
                    for chunk in response.iter_raw(chunk_size=8192):
                        file.write(chunk)
                        progress.update(task_id, advance=len(chunk))
            else:
                logging.error(f"Failed to download {url}. Status code: {response.status_code}")

    def download_files(self, urls, filepaths):
        with Progress(
            TextColumn("[bold blue]{task.description}", justify="right"),
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            DownloadColumn(),
            "•",
            TimeRemainingColumn(),
            "•",
            TransferSpeedColumn(),
        ) as progress:
            tasks = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                for url, filepath in zip(urls, filepaths):
                    task_description = f"Downloading {url.split('=')[-1]}"
                    task_id = progress.add_task(task_description, total=0)
                    tasks[task_id] = executor.submit(self.download_file, url, filepath, progress, task_id)
                for task_id, future in tasks.items():
                    # This will raise any exceptions that might have occurred
                    future.result()


def get_file_urls_and_paths(periods, file_types):
    urls = [
        f"http://10.173.224.100/History/BackupFilesGet.aspx?filename={period}-{file_type}.zip"
        for period in periods
        for file_type in file_types
    ]

    fns = [
        f"{BASE_FILE_PATH}/{file_type.upper()}/{period}-{file_type}.zip"
        for period in periods
        for file_type in file_types
    ]
    return urls, fns


def main(period):

    USERNAME = "s.atmani"  # Replace with your username
    PASSWORD = "65rBeY%$"  # Replace with your password

    # Define your periods and file types
    periods = pd.date_range(start=period, end=period, freq="MS").strftime("%Y-%m").tolist()
    file_types = ["met", "din", "grd", "cnt", "tur"]

    urls, fns = get_file_urls_and_paths(periods, file_types)

    login_manager = SimpleLoginManager()
    login_success = login_manager.login(USERNAME, PASSWORD)
    if login_success:
        print("Login was successful!")
        login_manager.download_files(urls, fns)
    else:
        print("Login failed.")


# Execute the main function
if __name__ == "__main__":
    setup_default_logger()
    main("2023-12")
