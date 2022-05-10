import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
from typing import Union, List


class SpreadSheetHandler:
    """
    Object for handling interactions with Google sheets
    Attributes:
        sheets: GSpread object for holding the google sheets document
    Methods:
        grab_sheet: method for grabbing a specific sheet from the sheets in a pandas DataFrame
        overwrite: method for writing a pandas DataFrame to a sheet
        resize_data: method for resizing the DataFrame so it doesn't go beyond the limit
    """

    def __init__(self, sheet_link: str, auth: gspread.client.Client):
        """
        Initializer method
        Arguments:
            sheet_link: the link to the google sheets document
            auth: google authorization key
        """
        self.sheets = auth.open_by_url(sheet_link)  # Grab the sheets from the link

    def grab_sheet(self, sheet_name: str) -> Union[pd.DataFrame, None]:
        """
        Method for grabbing the data from a given sheet as a pandas DataFrame
        Arguments:
            sheet_name: string, the name of the sheet ot grab
        Returns:
            The sheet with the given name, None if the name does not exist
        """
        try:
            sheet = self.sheets.worksheet(sheet_name)  # Grab the specified sheet
            sheet_df = pd.DataFrame(sheet.get_all_records())  # Format to pandas DataFrame
            return sheet_df
        except gspread.WorksheetNotFound:
            return None

    def overwrite(self, sheet_name: str, data: pd.DataFrame):
        """
        Method for overwriting all the data in a given sheet with a new dataframe
        Arguments:
            sheet_name: string, the name to save the sheet as
            data: The pandas DataFrame to save
        """
        try:
            sheet = self.sheets.worksheet(sheet_name)  # Grab the sheet
            sheet.clear()  # Clear the worksheet if it exists
        except gspread.WorksheetNotFound:
            self.sheets.add_worksheet(title=sheet_name, rows=data.shape[0], cols=data.shape[1])  # Otherwise reate the new sheet of a fitting size
            sheet = self.sheets.worksheet(sheet_name)  # Grab the fresh sheet
        set_with_dataframe(sheet, data, include_index=False, include_column_header=True)  # Save the dataframe
