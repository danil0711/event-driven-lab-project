from abc import ABC, abstractmethod


class Consumer(ABC):
    @abstractmethod
    def listen(self):
        pass

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass
