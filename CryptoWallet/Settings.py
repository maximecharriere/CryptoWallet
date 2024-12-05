import json
import os.path
from typing import ClassVar, Dict

class Settings:
    _default_values: ClassVar[dict] = {
        'root_dirpath': "C:/Users/Maxime/SynologyDrive/Documents/05_Crypto",
        'database_filepath': "Transactions/transactions.csv",
        'exported_transactions_dirpath': "ExportedTransactions",
        'output_dirpath': "Output",
        'cryptocompare_api_key': ""
    }
    _default_settings_filepath: ClassVar[str] = ".CryptoWallet/settings.json"

    def __init__(self, **kwargs):
        """Initialize Settings with given values or defaults."""
        for key, default_value in self._default_values.items():
            private_key = f"_{key}"
            value = kwargs.get(key, default_value)
            setattr(self, private_key, value)

        if not os.path.exists(self._default_settings_filepath):
            self.save()
            
    def _to_dict(self) -> Dict[str, str]:
        """Convert settings to dictionary format."""
        return {
            key: getattr(self, f"_{key}")
            for key in self._default_values.keys()
        }

    def __str__(self) -> str:
        """Return settings as formatted JSON string."""
        return json.dumps(self._to_dict(), indent=4)
    
    def save(self, filepath: str = _default_settings_filepath) -> None:
        """Save settings to a JSON file."""
        # Create directory to store settings file
        directory = os.path.dirname(filepath)
        if directory:  # Only create directories if there's actually a path component
            os.makedirs(directory, exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(self._to_dict(), f, indent=4)

    @classmethod
    def load(cls, filepath: str = _default_settings_filepath) -> 'Settings':
        """Load settings from a JSON file."""
        if not os.path.exists(filepath):
            return cls()
        with open(filepath, 'r') as f:
            data = json.load(f)
        return cls(**data)

    @property
    def root_dirpath(self) -> str:
        return self._root_dirpath
    
    @root_dirpath.setter
    def root_dirpath(self, value: str) -> None:
        self._root_dirpath = value
        self.save()

    @property
    def database_filepath(self) -> str:
        return os.path.join(self._root_dirpath, self._database_filepath)

    @property
    def exported_transactions_dirpath(self) -> str:
        return os.path.join(self._root_dirpath, self._exported_transactions_dirpath)
    
    @property
    def output_dirpath(self) -> str:
        return os.path.join(self._root_dirpath, self._output_dirpath)
    
    @output_dirpath.setter
    def output_dirpath(self, value: str) -> None:
        self._output_dirpath = value
        self.save()

    @property
    def cryptocompare_api_key(self) -> str:
        return self._cryptocompare_api_key

    @cryptocompare_api_key.setter
    def cryptocompare_api_key(self, value: str) -> None:
        self._cryptocompare_api_key = value
        self.save()