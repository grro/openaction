from abc import ABC, abstractmethod



class Subscription(ABC):

    @abstractmethod
    def notify(self, path: str):
        pass

