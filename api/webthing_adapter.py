from abc import ABC, abstractmethod


class WebThingAdapter(ABC):

    @abstractmethod
    def list_resources(self):
        """
        Retrieves a list of available resources exposed by the connected MCP server.

        Returns:
            ListResourcesResult: An object containing the available resources, including
                                 their URIs, names, and descriptions.
        """
        pass
