import sys
import pandas as pd
from PyQt6.QtWidgets import (QApplication, QMainWindow, QTableWidget, 
                           QTableWidgetItem, QVBoxLayout, QHBoxLayout,
                           QWidget, QLineEdit, QComboBox, QDialog, 
                           QCheckBox, QPushButton, QScrollArea, QLabel)
from PyQt6.QtCore import Qt, QSignalBlocker
from collections import defaultdict

from CryptoWallet.Loader import BinanceLoader, SwissborgLoader, KucoinLoader, BybitLoader, ManualTransactionsLoader, CoinbaseLoader
from CryptoWallet.Wallet import Wallet
from CryptoWallet.Settings import Settings


class FilterDialog(QDialog):
    def __init__(self, parent, column_values, column_name):
        super().__init__(parent)
        self.setWindowTitle(f"Filter {column_name}")
        self.setMinimumWidth(250)
        layout = QVBoxLayout()

        # Add search bar
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search values...")
        self.search_input.textChanged.connect(self.filter_checkboxes)
        layout.addWidget(self.search_input)

        # Create scrollable area for checkboxes
        scroll = QScrollArea()
        scroll_widget = QWidget()
        self.checkbox_layout = QVBoxLayout()

        # Modify Select All checkbox to allow tri-state
        self.select_all = QCheckBox("Select All")
        self.select_all.setTristate(True)
        self.select_all.checkStateChanged.connect(self.toggle_all)
        self.checkbox_layout.addWidget(self.select_all)

        # Add value checkboxes with counts
        self.checkboxes = {}
        value_counts = pd.Series(column_values).value_counts()
        for value in sorted(value_counts.index):
            count = value_counts[value]
            checkbox = QCheckBox(f"{value} ({count})")
            checkbox.setChecked(True)
            self.checkboxes[value] = checkbox
            self.checkbox_layout.addWidget(checkbox)

        scroll_widget.setLayout(self.checkbox_layout)
        scroll.setWidget(scroll_widget)
        scroll.setWidgetResizable(True)
        layout.addWidget(scroll)

        # Add checkbox state changed connections
        for checkbox in self.checkboxes.values():
            checkbox.stateChanged.connect(self.update_select_all_state)
        
        # Initialize select all state
        self.update_select_all_state()

        # Add OK/Cancel buttons
        buttons_layout = QVBoxLayout()
        ok_button = QPushButton("OK")
        ok_button.clicked.connect(self.accept)
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        buttons_layout.addWidget(ok_button)
        buttons_layout.addWidget(cancel_button)
        layout.addLayout(buttons_layout)

        self.setLayout(layout)

    def filter_checkboxes(self, text):
        for value, checkbox in self.checkboxes.items():
            checkbox.setVisible(text.lower() in str(value).lower())

    def update_select_all_state(self):
        checked_count = sum(1 for cb in self.checkboxes.values() if cb.isChecked())
        total_count = len(self.checkboxes)

        # Block signals to prevent recursion
        with QSignalBlocker(self.select_all):
            if checked_count == 0:
                self.select_all.setCheckState(Qt.CheckState.Unchecked)
            elif checked_count == total_count:
                self.select_all.setCheckState(Qt.CheckState.Checked)
            else:
                self.select_all.setCheckState(Qt.CheckState.PartiallyChecked)

    def toggle_all(self, state):
        # Convert PartiallyChecked to Checked
        check_state = True if state in (Qt.CheckState.PartiallyChecked, Qt.CheckState.Checked) else False
        
        # Block individual checkbox signals to prevent unnecessary updates
        for checkbox in self.checkboxes.values():
            with QSignalBlocker(checkbox):
                checkbox.setChecked(check_state)
        
        # Update select all state
        self.update_select_all_state()

    def get_selected_values(self):
        return {value for value, checkbox in self.checkboxes.items() 
                if checkbox.isChecked()}

class DataFrameTable(QTableWidget):
    def __init__(self, df):
        super().__init__()
        self.df = df
        self.filters = defaultdict(set)  # Column filters
        self.setDataFrame(df)
        self.setSortingEnabled(True)
        
        # Enable context menu for headers
        header = self.horizontalHeader()
        header.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        header.customContextMenuRequested.connect(self.show_filter_dialog)
    
    def show_filter_dialog(self, pos):
        column_index = self.horizontalHeader().logicalIndexAt(pos)
        column_name = self.horizontalHeaderItem(column_index).text()
        
        # Get all values in the column
        column_values = [self.item(row, column_index).text() 
                        for row in range(self.rowCount())]
        
        dialog = FilterDialog(self, column_values, column_name)
        if dialog.exec():
            self.filters[column_index] = dialog.get_selected_values()
            self.apply_filters()

    def apply_filters(self):
        for row in range(self.rowCount()):
            show_row = True
            for col, allowed_values in self.filters.items():
                value = self.item(row, col).text()
                if value not in allowed_values:
                    show_row = False
                    break
            self.setRowHidden(row, not show_row)

    def setDataFrame(self, df):
        self.setRowCount(len(df.index))
        self.setColumnCount(len(df.columns))
        self.setHorizontalHeaderLabels(df.columns)
        
        for i in range(len(df.index)):
            for j in range(len(df.columns)):
                value = str(df.iloc[i, j])
                item = QTableWidgetItem(value)
                self.setItem(i, j, item)

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("DataFrame Viewer")
        self.setGeometry(100, 100, 800, 600)
        
        self.settings = Settings.load()
        self.wallet = Wallet(apiKey=self.settings.cryptocompare_api_key, databaseFilename=self.settings.root_dirpath)
        
        # Create sample DataFrame
        data = {
            'Column' + str(i): [f'Value{i}-{j}' for j in range(10)]
            for i in range(5)
        }
        df = pd.DataFrame(data)
        
        # Simplified layout with just the table
        main_widget = QWidget()
        layout = QVBoxLayout()
        self.table = DataFrameTable(df)
        layout.addWidget(self.table)
        
        main_widget.setLayout(layout)
        self.setCentralWidget(main_widget)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
