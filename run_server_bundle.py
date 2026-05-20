import sys
import logging
from service_registry import Configs
from openaction_server import OpenActionServer
from opendiscovery_server import OpenDiscoveryServer








if __name__ == '__main__':
    # Globally setup format and log level for the application root
    logging.basicConfig(format='%(asctime)s %(name)-20s: %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    # Silence chatty third-party modules
    logging.getLogger('tornado.access').setLevel(logging.ERROR)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
    logging.getLogger('starlette.middleware.base').setLevel(logging.WARNING)
    logging.getLogger('fastmcp').setLevel(logging.WARNING)
    logging.getLogger('uvicorn.access').disabled = True
    logging.getLogger('uvicorn.error').setLevel(logging.WARNING)
    logging.getLogger('uvicorn').setLevel(logging.WARNING)

    port = int(sys.argv[1])
    work_dir = sys.argv[2]
    config = Configs.read(sys.argv[3])


    opendiscovery = OpenDiscoveryServer('OpenDiscovery', port, work_dir, config)
    opendiscovery.start()

    openaction = OpenActionServer('OpenAction', port+1, work_dir)
    openaction.start_ant_wait()



# npx @modelcontextprotocol/inspector
# http://localhost:6274/