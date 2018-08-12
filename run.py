import threading

from SpiderServer.SpiderServer import SpiderServer

if __name__ == '__main__':
    server = SpiderServer()
    threading.Thread(target=server.serve).start()