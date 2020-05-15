from binance.client import Client
from binance.websockets import BinanceSocketManager


def process_message(msg):
    if msg['e'] == 'error':
        # close and restart the socket
        print('Socket error')
    else:
        # process message normally
        print('Message received')
        print(msg)

if __name__ == '__main__':

    api_key = ''
    api_secret = ''
    client = Client(api_key, api_secret)
    bm = BinanceSocketManager(client)
    # start any sockets here, i.e a trade socket
    conn_key = bm.start_trade_socket('BTCUSDT', process_message)
    # then start the socket manager
    bm.start()

