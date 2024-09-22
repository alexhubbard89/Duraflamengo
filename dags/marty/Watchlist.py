import os
import pandas as pd
import datetime as dt
import uuid


import os
import pandas as pd
import uuid


class Watchlist:
    """
    Manages a watchlist including functionalities to display, add, and remove tickers,
    as well as to manage technical prospecting.
    """

    def __init__(self):
        self.DL_DIR = os.environ.get(
            "DL_DIR", "./"
        )  # Use the current directory if DL_DIR is not set
        self.WATCHLIST_FILE = os.path.join(self.DL_DIR, "marty/watchlist/data.parquet")
        self.PROSPECTING_NOTES_FILE = os.path.join(
            self.DL_DIR, "marty/watchlist/prospecting_notes.parquet"
        )
        self.SIMULATION_LOG_FILE = os.path.join(
            self.DL_DIR, "marty/watchlist/simulation_log.parquet"
        )
        self.refresh_watchlist(extended=False)
        self.refresh_simulation()
        self.refresh_active_prospects()

    def refresh_watchlist(self, extended: bool = True) -> pd.DataFrame:
        """
        Displays the watchlist.

        Args:
            extended (bool): Whether to display the extended watchlist or just the base.

        Returns:
            A pandas DataFrame containing the requested watchlist.
        """
        df = pd.read_parquet(self.WATCHLIST_FILE).sort_values(
            "symbol", ignore_index=True
        )
        if extended:
            self.df = df
        else:
            self.df = df[df["type"] == "base"]

    def refresh_prospecting(self) -> pd.DataFrame:
        """
        Displays the watchlist.

        Args:
            extended (bool): Whether to display the extended watchlist or just the base.

        Returns:
            A pandas DataFrame containing the requested watchlist.
        """
        self.prospecting_notes_df = pd.read_parquet(
            self.PROSPECTING_NOTES_FILE
        ).sort_values(["symbol", "monitor_start_date"], ignore_index=True)

    def refresh_active_prospects(self) -> pd.DataFrame:
        """ "
        Displays the active prospects.

        Returns:
            A pandas DataFrame containing the requested active prospects.
        """
        self.refresh_prospecting()
        self.active_prospecting_notes_df = self.prospecting_notes_df.loc[
            self.prospecting_notes_df["active"] == True
        ]

    def refresh_simulation(self) -> pd.DataFrame:
        """
        Displays the watchlist.

        Args:
            extended (bool): Whether to display the extended watchlist or just the base.

        Returns:
            A pandas DataFrame containing the requested watchlist.
        """
        self.simulation_df = pd.read_parquet(self.SIMULATION_LOG_FILE)

    def generate_unique_uuid(self, dataset):
        """
        Generates a unique UUID that does not exist in the simulation DataFrame's 'uuid' column.

        This function repeatedly generates a new UUID until it finds one that is not already
        present in the 'uuid' column of the self.simulation_df DataFrame. It utilizes a while
        loop to ensure uniqueness and only exits when a unique UUID is found. The simulation
        data is refreshed at the beginning of each iteration to ensure the uniqueness check
        is up-to-date.

        Returns:
            str: A unique UUID as a string.
        """
        if dataset == "simulation":
            self.refresh_simulation()
            data = self.simulation_df
        elif dataset == "prospecting":
            self.refresh_prospecting()
            data = self.prospecting_notes_df

        if data.empty:
            return str(uuid.uuid4())

        while True:
            uuid_ = str(uuid.uuid4())
            if uuid_ not in data:
                break
        return uuid_

    def add_to_watchlist(self, symbol: str):
        """
        Adds a symbol to the watchlist.

        Args:
            symbol (str): The symbol to add.
            extended (bool): Whether to add the symbol to the extended watchlist.
        """
        self.refresh_watchlist()
        new_row = {"symbol": symbol, "type": "extended", "technical_prospect": False}
        self.df = self.df.append(new_row, ignore_index=True).drop_duplicates()
        self.df.to_parquet(self.WATCHLIST_FILE, index=False)
        self.refresh_watchlist()

    def remove_from_watchlist(self, symbol: str):
        """
        Removes a symbol from the watchlist.

        Args:
            symbol (str): The symbol to remove.
        """
        self.refresh_watchlist()
        self.df = self.df[self.df["symbol"] != symbol]
        self.df.to_parquet(self.WATCHLIST_FILE, index=False)
        self.refresh_watchlist()

    def add_to_technical_prospecting(
        self,
        symbol,
        prospecting_type,
        monitor_start_date,
        contract_type,
        entry_point=None,
        stop_loss=None,
        price_target=None,
        days_to_target=None,
        notes="",
    ):
        """
        Marks a symbol as a technical prospect and creates a new entry in the prospecting notes dataset.

        Args:
            symbol (str): The symbol to mark as a technical prospect.
            prospecting_type (str): 'trading' or 'testing'.
            monitor_start_date (str or datetime): The start date for monitoring this prospect.
            contract_type (str): 'CALL' or 'PUT'.
            entry_point (float, optional): The entry point price.
            stop_loss (float, optional): The stop loss price.
            price_target (float, optional): The target price for the trade.
            days_to_target (int, optional): Expected days to reach the target price.
            notes (str, optional): Additional notes regarding the prospecting entry.
        """
        # Update watchlist dataset
        self.refresh_watchlist()
        if symbol not in self.df["symbol"].values:
            raise ValueError(f"Symbol {symbol} not found in watchlist.")

        # Add entry to prospecting notes dataset
        self.add_prospecting_entry(
            symbol,
            prospecting_type,
            monitor_start_date,
            contract_type,
            entry_point,
            stop_loss,
            price_target,
            days_to_target,
            notes,
            active=True,
        )

        # No error in notes entry. Update watchlist
        if prospecting_type == "trading":
            self.df.loc[self.df["symbol"] == symbol, "technical_prospect"] = True
            self.df.to_parquet(self.WATCHLIST_FILE, index=False)
            self.refresh_watchlist()

    def remove_from_technical_prospecting(
        self, symbol, prospecting_type, monitor_start_date, contract_type
    ):
        """
        Removes a symbol from being marked as a technical prospect and updates the prospecting notes dataset.

        Args:
            symbol (str): The symbol to unmark.
        """
        # Update watchlist dataset
        self.refresh_watchlist(extended=True)
        if symbol not in self.df["symbol"].values:
            raise ValueError(f"Symbol {symbol} not found in watchlist.")

        # Set active to False in prospecting notes dataset for the symbol
        self.set_prospecting_entry_inactive(
            symbol, prospecting_type, monitor_start_date, contract_type
        )

        # No error in notes entry. Update watchlist
        if prospecting_type == "trading":
            self.df.loc[self.df["symbol"] == symbol, "technical_prospect"] = False
            self.df.to_parquet(self.WATCHLIST_FILE, index=False)
            self.refresh_watchlist()

    def add_prospecting_entry(
        self,
        symbol,
        prospecting_type,
        monitor_start_date,
        contract_type,
        entry_point=None,
        stop_loss=None,
        price_target=None,
        days_to_target=None,
        notes="",
        active=True,
    ):
        """
        Adds a new prospecting entry to the dataset.

        Args:
            symbol (str): The symbol being prospected.
            prospecting_type (str): 'trading' or 'testing'.
            monitor_start_date (str or datetime): The start date for monitoring this prospect.
            contract_type (str): 'CALL' or 'PUT'.
            entry_point (float, optional): The entry point price.
            stop_loss (float, optional): The stop loss price.
            price_target (float, optional): The target price for the trade.
            days_to_target (int, optional): Expected days to reach the target price.
            notes (str, optional): Additional notes regarding the prospecting entry.
            active (bool, optional): If the prospecting entry is active. Defaults to True.
        """
        prospecting_notes_df = pd.read_parquet(self.PROSPECTING_NOTES_FILE)

        # Check for duplicates in trading and active entries
        if prospecting_type == "trading" and active:
            duplicate_entries = prospecting_notes_df[
                (prospecting_notes_df["symbol"] == symbol)
                & (prospecting_notes_df["prospecting_type"] == prospecting_type)
                & (
                    prospecting_notes_df["monitor_start_date"]
                    == pd.to_datetime(monitor_start_date)
                )
                & (prospecting_notes_df["contract_type"] == contract_type)
                & (prospecting_notes_df["active"] == True)
            ]
            if not duplicate_entries.empty:
                raise ValueError("Duplicate active trading entry detected.")

        # Add new entry
        new_entry = {
            "uuid": self.generate_unique_uuid(dataset="prospecting"),
            "symbol": symbol,
            "prospecting_type": prospecting_type,
            "active": active,
            "monitor_start_date": pd.to_datetime(monitor_start_date),
            "contract_type": contract_type,
            "entry_point": entry_point,
            "stop_loss": stop_loss,
            "price_target": price_target,
            "days_to_target": days_to_target,
            "notes": notes,
            "datetime_added": dt.datetime.now(),
        }
        prospecting_notes_df = prospecting_notes_df.append(new_entry, ignore_index=True)
        prospecting_notes_df.to_parquet(self.PROSPECTING_NOTES_FILE, index=False)
        self.refresh_prospecting()

    def set_prospecting_entry_inactive(
        self, symbol, prospecting_type, monitor_start_date, contract_type
    ):
        """
        Sets the active status of a prospecting entry to False.

        Args:
            symbol (str): The symbol being prospected.
            prospecting_type (str): 'trading' or 'testing'.
            monitor_start_date (str or datetime): The start date for monitoring this prospect.
            contract_type (str): 'CALL' or 'PUT'.
        """
        prospecting_notes_df = pd.read_parquet(self.PROSPECTING_NOTES_FILE)
        indices = prospecting_notes_df[
            (prospecting_notes_df["symbol"] == symbol)
            & (prospecting_notes_df["prospecting_type"] == prospecting_type)
            & (
                prospecting_notes_df["monitor_start_date"]
                == pd.to_datetime(monitor_start_date)
            )
            & (prospecting_notes_df["contract_type"] == contract_type)
        ].index

        if not indices.empty:
            prospecting_notes_df.loc[indices, "active"] = False
            prospecting_notes_df.to_parquet(self.PROSPECTING_NOTES_FILE, index=False)
            self.refresh_prospecting()
        else:
            raise ValueError("Entry not found.")

    def update_prospecting_entry(
        self,
        uuid_=None,
        active=None,
        monitor_start_date=None,
        contract_type=None,
        entry_point=None,
        stop_loss=None,
        price_target=None,
        days_to_target=None,
        notes=None,
    ):
        """
        Updates an existing prospecting entry in the dataset using its unique identifier.

        Args:
            uuid_ (str): The UUID of the prospecting entry to update.
            entry_point (float, optional): The entry point price.
            stop_loss (float, optional): The stop loss price.
            price_target (float, optional): The target price for the trade.
            days_to_target (int, optional): Expected days to reach the target price.
            notes (str, optional): Additional notes regarding the prospecting entry.
        """
        prospecting_notes_df = pd.read_parquet(self.PROSPECTING_NOTES_FILE)
        # if uuid_ == None:
        #     pass
        # elif uuid_ not in prospecting_notes_df["uuid"].values:
        #     raise ValueError(f"UUID {uuid_} not found in prospecting entries.")

        # Get the index of the entry to update
        index = prospecting_notes_df.loc[prospecting_notes_df["uuid"] == uuid_].index

        # Update only specified fields
        if active is not None:
            prospecting_notes_df.at[index, "active"] = active
        if monitor_start_date is not None:
            prospecting_notes_df.at[index, "monitor_start_date"] = monitor_start_date
        if contract_type is not None:
            prospecting_notes_df.at[index, "contract_type"] = contract_type
        if entry_point is not None:
            prospecting_notes_df.at[index, "entry_point"] = entry_point
        if stop_loss is not None:
            prospecting_notes_df.at[index, "stop_loss"] = stop_loss
        if price_target is not None:
            prospecting_notes_df.at[index, "price_target"] = price_target
        if days_to_target is not None:
            prospecting_notes_df.at[index, "days_to_target"] = days_to_target
        if notes is not None:
            prospecting_notes_df.at[index, "notes"] = notes

        # Save the updated DataFrame
        prospecting_notes_df.to_parquet(self.PROSPECTING_NOTES_FILE, index=False)
        self.refresh_prospecting()

    def log_simulation_result(
        self,
        symbol,
        simulation_start_date,
        simulation_duration,
        simulation_result,
        prospecting_type,
        active,
        monitor_start_date,
        contract_type,
        entry_point=None,
        stop_loss=None,
        price_target=None,
        days_to_target=None,
        notes="",
        approved=False,
    ):
        """Logs the result of a simulation, ensuring no duplicates are added."""
        # Ensure the simulation log is up to date
        self.refresh_simulation()

        # Define a filter to check for duplicate entries
        duplicate_filter = (
            (self.simulation_df["symbol"] == symbol)
            & (self.simulation_df["simulation_start_date"] == simulation_start_date)
            & (self.simulation_df["simulation_duration"] == simulation_duration)
            & (self.simulation_df["prospecting_type"] == prospecting_type)
            & (self.simulation_df["monitor_start_date"] == monitor_start_date)
            & (self.simulation_df["contract_type"] == contract_type)
        )

        # Check for duplicates
        if not self.simulation_df[duplicate_filter].empty:
            # Duplicate entry found, do not add the new entry
            return

        new_entry = {
            "uuid": self.generate_unique_uuid(dataset="simulation"),
            "symbol": symbol,
            "simulation_start_date": simulation_start_date,
            "simulation_duration": simulation_duration,
            "simulation_result": simulation_result,
            "prospecting_type": prospecting_type,
            "active": active,
            "monitor_start_date": monitor_start_date,
            "contract_type": contract_type,
            "entry_point": entry_point,
            "stop_loss": stop_loss,
            "price_target": price_target,
            "days_to_target": days_to_target,
            "notes": notes,
            "approved": approved,
        }
        # Append new entry
        self.simulation_df = self.simulation_df.append(new_entry, ignore_index=True)
        # Save back to file
        self.simulation_df.to_parquet(self.SIMULATION_LOG_FILE, index=False)
        self.refresh_simulation()

    def update_simulation_entry(self, simulation_uuid: str, **kwargs):
        """
        Updates fields of a simulation entry identified by its UUID.

        Args:
            simulation_uuid (str): The UUID of the simulation to update.
            **kwargs: Arbitrary keyword arguments representing the fields to update and their new values.
        """
        # Ensure the simulation log is up to date
        self.refresh_simulation()

        # Find the row index of the simulation to update
        index = self.simulation_df.index[self.simulation_df["uuid"] == simulation_uuid]

        if index.empty:
            # Raise an error if the UUID is not found
            raise ValueError(f"Simulation UUID {simulation_uuid} not found.")

        # Loop through each item in kwargs and update the corresponding field
        for field, value in kwargs.items():
            if field in self.simulation_df.columns:
                self.simulation_df.at[index, field] = value
            else:
                # Optionally, raise an error or warning if the field does not exist
                print(
                    f"Warning: Field '{field}' does not exist in the simulation DataFrame and will be ignored."
                )

        # Save the updated DataFrame back to the parquet file
        self.simulation_df.to_parquet(self.SIMULATION_LOG_FILE, index=False)

        # Refresh the simulation data to reflect the changes
        self.refresh_simulation()

    def remove_simulation_entry(self, simulation_uuid: str):
        """
        Removes a simulation entry from the simulation DataFrame identified by its UUID.

        Args:
            simulation_uuid (str): The UUID of the simulation to remove.
        """
        # Ensure the simulation data is up to date
        self.refresh_simulation()

        # Remove the simulation entry from the DataFrame
        self.simulation_df = self.simulation_df[
            self.simulation_df["uuid"] != simulation_uuid
        ]

        # Save the updated DataFrame back to the parquet file
        self.simulation_df.to_parquet(self.SIMULATION_LOG_FILE, index=False)

        # Refresh the simulation data to ensure the DataFrame is up to date
        self.refresh_simulation()
