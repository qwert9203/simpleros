import multiprocessing  # for intra process communication
import socket  # for inter process communication
import asyncio
import sys
import time
from contextlib import closing
import json
import queue
import logging
import logging.handlers

# TODO: add color code customization
COLORS = {"DEBUG": 92,
          "INFO": 39,
          "WARNING": 33,
          "ERROR": 35,
          "CRITICAL": 31
          }


class ColorFormatter(logging.Formatter):

    def __init__(self, msg, use_color=True):
        logging.Formatter.__init__(self, msg)
        self.use_color = use_color

    def format(self, record):
        levelname = record.levelname
        if self.use_color and levelname in COLORS:
            levelname_color = "\033[%dm" % COLORS[levelname] + levelname + "\033[0m"
            record.levelname = levelname_color
        return logging.Formatter.format(self, record)


def find_free_port():
    """ Finds a free port on the local device using sockets.

    :return: a free port number on the local device
    """

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def get_local_ip():
    return socket.gethostbyname(socket.gethostname())


def subscribe(topic: str):
    assert topic.find(" ") == -1, f"topic \"{topic}\" should not contain spaces"

    def s_w(func):
        def w(*args, __val__=None, __checking__=False, **kwargs):  # args[0] must be the node
            if __checking__:
                assert __val__ == "subscribe"
            x = args[0].sub(topic, func.__name__)  # tries to subscribe
            if x == -1:  # cannot subscribe
                args[0].warning("cannot subscribe!")
            elif x == 0:  # already subscribed
                func(*args, **kwargs)
            else:  # not subscribed, does not run function
                if not is_dunder(topic):
                    args[0].print(f"{args[0].name} subscribed to {topic}")

        return w

    return s_w


def loop(time_ms):
    def l_w(func):
        def wrapper(*args, __val__=None, __checking__=False):  # args[0] must be the node
            if __checking__:
                assert __val__ == "loop"
            args[0].loops.append({"time": time_ms, "func": func})

        return wrapper

    return l_w


def get_address(i):
    return (i[0], int(i[1]))


# function that checks if the input is in the form of __*__
def is_dunder(x: str):
    try:
        y = x[0] + x[1] + x[-2] + x[-1]
        return y == "____"
    except:
        return False


def run_io_listener(q, port: tuple):
    listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    listener.bind(get_address(port))
    # print(f"listener bound to {port}")
    while True:
        data, addr = listener.recvfrom(1024)  # 1024 bytes, addr is not used here
        q.put(data)


def run_io_sender(to_send, port: tuple):
    writer = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    writer.bind(get_address(port))
    # print(f"sender bound to {port}")
    while True:
        if not to_send.empty():
            msg = to_send.get()
            if isinstance(msg[1][0], list):
                for recv_port in msg[1]:
                    writer.sendto(msg[0], get_address(recv_port))
            else:
                writer.sendto(msg[0], get_address(msg[1]))


# main node class
class Node:
    """ The main object


    """

    def __init__(self, name: str, recv_port=None, send_port=None, master_port=None):
        if send_port is None:
            send_port = [get_local_ip(), find_free_port()]
        if recv_port is None:
            recv_port = [get_local_ip(), find_free_port()]
        assert master_port is not None, "The ip of the master node needs to be known."
        self.name = name
        self.recvport = recv_port  # port for receiving data
        self.sendport = send_port  # port for sending data
        self.master_port = master_port  # port to send data to
        self.subscriptions = {}  # stores subscription data and what methods to call
        self.to_send = multiprocessing.Manager().Queue()  # for transferring data from publish() to sender
        self.to_process = multiprocessing.Manager().Queue()  # for transferring data from listener to run()
        self.processes = []
        self.is_alive = True
        self.connected = False
        self.loops = []
        # logging
        self.msg_queue = queue.Queue(-1)  # no limit on size
        self.msg_queue_handler = logging.handlers.QueueHandler(self.msg_queue)
        self.msg_handler = logging.StreamHandler(sys.stdout)
        self.msg_listener = logging.handlers.QueueListener(self.msg_queue, self.msg_handler)
        self.logger = logging.getLogger(self.name)
        self.logger.addHandler(self.msg_queue_handler)
        # TODO: add customization to formatters
        self.logger.setLevel(logging.DEBUG)
        self.formatter = ColorFormatter('[%(name)s] %(levelname)s: %(message)s')
        self.msg_handler.setFormatter(self.formatter)
        self.msg_listener.start()

    def _handle_msg(self, r):
        # decoding
        try:
            result = json.loads(r.decode("utf-8"))  # client feedback
            topic = result["topic"]
            data = result["data"]
            addr = result["addr"]
            recp = result["recp"]
        except json.decoder.JSONDecodeError:
            self.error("cannot decode!")
            topic = "__error__"
            data = "error"
            addr = ("error", "error")
            recp = ""
        self._on_receive(topic, data, addr, recp)

    # to be called whenever this node receives any data
    def _on_receive(self, topic, data, port, recp):
        if self.is_subscribed(topic):
            self._callback(topic, data, port)
        else:
            self.warning(f"{self.name} is not subscribed to {topic}!")

    def _callback(self, topic, data, port):
        try:
            self.subscriptions[topic](data, port)
        except:
            self.error(f"error executing {topic}, {self.name}")

    # returns all subscriber callback method names
    def _get_sub_cb(self):
        return [f for f in dir(self) if callable(getattr(self, f)) and not is_dunder(f)]

    # adds subscriptions to self
    def _add_sub(self):
        cbs = self._get_sub_cb()
        for cb in cbs:
            try:  # only throws error when the function isn't subscribe
                getattr(self, cb)(self, __val__="subscribe", __checking__=True)
            except:  # subscribe
                pass

    def _add_loop(self):
        cbs = self._get_sub_cb()
        for cb in cbs:
            try:
                getattr(self, cb)(self, __val__="loop", __checking__=True)
            except:
                pass

    async def _timer(self, time_ms, func):
        while self.is_alive:
            func(self)
            await asyncio.sleep(time_ms / 1000)

    # initializes the node completely (sends self subscriptions to master)
    # then starts sender and listener processes
    # finally starts the main loop of parsing information in to_process to _on_receive
    async def _run(self):
        self.print(f"{self.name} has an id of {id(self)}")
        # adds subscriptions to self
        self._add_sub()
        # creates connection to master node
        subs = list(self.subscriptions.keys())
        listener = multiprocessing.Process(target=run_io_listener, args=[self.to_process, self.recvport], daemon=True,
                                           name=f"listener: {self.name}")
        sender = multiprocessing.Process(target=run_io_sender, args=[self.to_send, self.sendport], daemon=True,
                                         name=f"sender: {self.name}")
        self.processes.append(listener)
        self.processes.append(sender)

        listener.start()
        sender.start()

        self._add_loop()
        # echoes master
        while True:
            self.publish("__echo__", self.name, recipients="master")
            await asyncio.sleep(1)
            if not self.to_process.empty():
                msg = self.to_process.get()
                self._handle_msg(msg)
                await asyncio.sleep(1)
            if not self.connected:
                self.warning("could not connect to master, trying again in 1 second")
            else:
                break

        self.publish("__sub__", subs, recipients="master")
        # sending data to individual processes (async)
        for loop in self.loops:
            asyncio.create_task(self._timer(loop["time"], loop["func"]))
        while self.is_alive:
            if not self.to_process.empty():
                msg = self.to_process.get()
                self._handle_msg(msg)
            await asyncio.sleep(0)

    # returns: -1 = error, 0 = already subbed, 1 = first time sub
    def sub(self, topic, cbfunc_name: str):
        try:
            if self.subscriptions.get(topic):
                return 0
            else:
                self.subscriptions[topic] = getattr(self, cbfunc_name)
                return 1
        except AttributeError as e:
            self.error(f"function \"{cbfunc_name}\" not found!")
            return -1

    def is_subscribed(self, topic):
        return self.subscriptions.get(topic) is not None

    # call asyncio
    def run(self):
        asyncio.run(self._run())

    # stops all nodes, including the master
    def stopall(self):
        self.publish("__stop__", "stop", recipients="__global__")

    # stops the node and unsubscribes from all topics
    def stop(self):
        self.print(f"stopping {self.name} at {time.time()}")
        self.publish("__unsub__", list(self.subscriptions.keys()))
        for process in self.processes:
            process.terminate()
            self.is_alive = False

    # publishes topic with data to self.to_send
    def publish(self, topic, data, port=None, recipients=None):
        port = port if port else self.recvport
        recp = recipients if recipients else "__subs__"
        msg = json.dumps({"topic": topic, "data": data, "addr": port, "recp": recp}).encode("utf-8") + b"\n"
        self.to_send.put([msg, self.master_port])

    def print(self, msg, level="INFO"):
        levels = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}
        if level.upper() in levels:
            self.logger.log(levels[level.upper()], msg)
        else:
            self.logger.log(10, msg)

    def debug(self, msg):
        self.logger.debug(msg)

    def info(self, msg):
        self.logger.info(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)

    def critical(self, msg):
        self.logger.critical(msg)

    @subscribe("__echo__")
    def _echo(self, data, port):
        self.publish("__echoresp__", data, recipients=port)

    @subscribe("__echoresp__")
    def _echoresp(self, data, port):
        self.connected = True

    @subscribe("__stop__")
    def _stop(self, data, port):
        self.stop()

    @subscribe("__error__")
    def _error(self, data, port):
        self.error("error decoding data!")


class LocalNode(Node):
    def __init__(self, name):
        super().__init__(name, master_port=[get_local_ip(), "12222"])


# global master
class MasterNode(Node):

    def __init__(self):
        super().__init__("master", recv_port=[get_local_ip(), "12222"], send_port=[get_local_ip(), "22222"],
                         master_port="1")
        self.topics = {}  # {topic names: subscriber id: str}
        self.nodenames = {}

    def translate_recp(self, recp, topic):
        """ translates recipients names to their ips

        :return:
        """
        if recp == self.name:
            return self.name
        if recp is list:
            return filter(lambda x: x is not None, [self.translate_recp(x, topic) for x in recp])
        else:
            if is_dunder(recp):
                return recp
            try:
                return self.nodenames[recp]
            except KeyError:
                self.warning(f"node {recp} does not exist!")

    # initializes the node completely (sends self subscriptions to master)
    # then starts sender and listener processes
    # finally starts the main loop of parsing information in to_process to _on_receive
    async def _run(self):
        self.print(f"{self.name} has an id of {id(self)}")
        self._add_sub()
        listener = multiprocessing.Process(target=run_io_listener, args=[self.to_process, self.recvport])
        sender = multiprocessing.Process(target=run_io_sender, args=[self.to_send, self.sendport])
        self.processes.append(listener)
        self.processes.append(sender)
        listener.start()
        sender.start()
        # sending data to individual processes (async)
        while self.is_alive:
            if not self.to_process.empty():
                msg = self.to_process.get()
                self._handle_msg(msg)
            # await asyncio.sleep(0)

    def _on_receive(self, topic, data, port, recp):
        r = self.translate_recp(recp, topic)
        if r == self.name:  # self is the only recipient
            self._callback(topic, data, port)
        elif r == "__subs__":
            if self.topics.get(topic):
                self.publish(topic, data, port=port, recipients=self.topics[topic])
            else:
                self.warning(f"topic {topic} has no recipients!")
            if self.subscriptions.get(topic):
                self._callback(topic, data, port)
        elif r == "__all__":  # master only passes on the message
            self.publish(topic, data, port=port, recipients=self.get_all_subs())
        elif r == "__global__":
            self.publish(topic, data, port=port, recipients=self.get_all_subs())
            self._callback(topic, data, port)
        elif r:
            self.publish(topic, data, port=port, recipients=r)

    # returns all nodes that are subscribed to any topic
    def get_all_subs(self):
        uniques = []
        for i in self.topics.values():
            for j in i:
                if j not in uniques:
                    uniques.append(j)
        return uniques

    def stop(self):
        time.sleep(1)
        for process in self.processes:
            process.terminate()
        self.is_alive = False

    def publish(self, topic, data, port=None, recipients=None):
        port = port if port else self.recvport
        recp = recipients if recipients else self.get_all_subs()
        msg = json.dumps({"topic": topic, "data": data, "addr": port, "recp": recp}).encode("utf-8") + b"\n"
        self.to_send.put([msg, recp])

    @subscribe("__sub__")
    def _sub(self, data, port):
        # subscriptions
        self.print(f"setting up subscription data for {port}")
        for t in data:
            try:
                self.topics[t].append(port)
            except KeyError:
                self.topics[t] = [port]

    @subscribe("__unsub__")
    def _unsub(self, data, port):
        for topic, ports in self.topics.items():
            if topic in data:
                ports.remove(port)

    @subscribe("__echo__")
    def _echo(self, data, port):
        self.publish("__echoresp__", data, recipients=port)
        self.nodenames[data] = port


def main():
    master = MasterNode()
    master.run()


if __name__ == "__main__":
    main()
