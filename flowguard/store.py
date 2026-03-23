import json
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

class BaseStore(ABC):
    @abstractmethod
    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    async def set(self, key: str, value: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    async def delete(self, key: str) -> None:
        pass

class MemoryStore(BaseStore):
    def __init__(self):
        self._data: Dict[str, Dict[str, Any]] = {}

    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        return self._data.get(key)

    async def set(self, key: str, value: Dict[str, Any]) -> None:
        self._data[key] = value

    async def delete(self, key: str) -> None:
        self._data.pop(key, None)

class JSONStore(BaseStore):
    def __init__(self, path: str = "flowguard_state.json"):
        self.path = path
        if not os.path.exists(path):
            with open(path, 'w') as f:
                json.dump({}, f)

    def _load(self) -> Dict:
        with open(self.path, 'r') as f:
            return json.load(f)

    def _save(self, data: Dict):
        with open(self.path, 'w') as f:
            json.dump(data, f, indent=2)

    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        data = self._load()
        return data.get(key)

    async def set(self, key: str, value: Dict[str, Any]) -> None:
        data = self._load()
        data[key] = value
        self._save(data)

    async def delete(self, key: str) -> None:
        data = self._load()
        if key in data:
            del data[key]
            self._save(data)