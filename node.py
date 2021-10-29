import multiprocessing  # for intra process communication
import socket  # for inter process communication
import json
import time  # bruh
import asyncio
import serial


# subscribes to the topic if not subscribed else runs the function
def subscribe(topic: str):
    assert topic.find(" ") == -1, f"topic \"{topic}\" should not contain spaces"

    def s_w(func):
        def w(*args, __val__=None, __checking__=False, **kwargs):  # args[0] must be the node
            if __checking__:
                assert __val__ == "subscribe"
            x = args[0].sub(topic, func.__name__)  # tries to subscribe
            if x == -1:  # cannot subscribe
                print("cannot subscribe!")
            elif x == 0:  # already subscribed
                func(*args, **kwargs)
            else:  # not subscribed, does not run function
                if not is_dunder(topic):
                    print(f"{args[0].name} subscribed to {topic}")

        return w

    return s_w

def loop(time_ms):
    def l_w(func):
        def wrapper(*args, __val__=None, __checking__=False, **kwargs):  # args[0] must be the node
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
    print(f"listener bound to {port}")
    while True:
        data, addr = listener.recvfrom(1024)  # 1024 bytes, addr is not used here
        print(f"listener received: {data}")
        q.put(data)


def run_io_sender(to_send, port: tuple):
    writer = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    writer.bind(get_address(port))
    print(f"sender bound to {port}")
    while True:
        if not to_send.empty():
            msg = to_send.get()
            if isinstance(msg[1][0], list):
                for recv_port in msg[1]:
                    writer.sendto(msg[0], get_address(recv_port))
            else:
                writer.sendto(msg[0], get_address(msg[1]))

def run_serial_listener(q, port):
    listener = serial.Serial(port=port[1], baudrate=115200, timeout=.1)
    while True:
        data = listener.readline()
        q.put(data)


# main node class
class Node:

    def __init__(self, name, recv_port=None, send_port=None, master_port=["127.0.0.1", "12222"]):
        assert recv_port is not None and send_port is not None
        self.statics = ["on_receive", "publish", "sub", "get_sub_cb", "run", "_run", "_add_loop",
                        "_add_sub", "handle_msg", "stopall"]
        self.name = name
        self.recvport = recv_port  # port for receiving data
        self.sendport = send_port  # port for sending data
        self.master_port = master_port  # port to send data to
        self.subscriptions = {}  # stores subscription data and what methods to call
        self.to_send = multiprocessing.Manager().Queue()  # for transferring data from publish() to sender
        self.to_process = multiprocessing.Manager().Queue()  # for transferring data from listener to run()
        self.to_publish = multiprocessing.Manager().Queue()  # for transferring data from loops to publish()
        self.processes = []
        self.is_alive = True
        self.connected = False
        self.loops = []

    def handle_msg(self, r):
        # decoding
        try:
            result = json.loads(r.decode("utf-8"))  # client feedback
            topic = result["topic"]
            data = result["data"]
            addr = result["addr"]
        except json.decoder.JSONDecodeError:
            print("cannot decode!")
            topic = "__error__"
            data = "error"
            addr = ("error", "error")
        try:
            self.on_receive(topic, data, addr)
        except KeyError:
            print(f"{self.name} is not subscriber to {topic}!")

    # to be called whenever this node receives any data
    def on_receive(self, topic, data, port):
        self.subscriptions[topic](data, port)

    # publishes topic with data to self.to_send
    def publish(self, topic, data):
        self.to_send.put(
            [json.dumps({"topic": topic, "data": data, "addr": self.recvport}).encode("utf-8") + b"\n", self.master_port])

    # returns all subscriber callback method names
    def get_sub_cb(self):
        return [f for f in dir(self) if callable(getattr(self, f)) and not is_dunder(f) and f not in self.statics]

    def stopall(self):
        self.publish("__stop__", "stop")

    # adds subscriptions to self
    def _add_sub(self):
        cbs = self.get_sub_cb()
        for cb in cbs:
            try: # only throws error when the function isn't subscribe
                getattr(self, cb)(self, __val__="subscribe", __checking__=True)
            except: # subscribe
                pass

    def _add_loop(self):
        cbs = self.get_sub_cb()
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
    # finally starts the main loop of parsing information in to_process to on_receive
    async def _run(self):
        print(f"{self.name} has an id of {id(self)}")
        # adds subscriptions to self
        self._add_sub()
        # creates connection to master node
        subs = list(self.subscriptions.keys())
        listener = multiprocessing.Process(target=run_io_listener, args=[self.to_process, self.recvport])
        sender = multiprocessing.Process(target=run_io_sender, args=[self.to_send, self.sendport])
        self.processes.append(listener)
        self.processes.append(sender)
        listener.start()
        sender.start()

        self._add_loop()
        # echoes master
        while True:
            self.publish("__echo__", self.master_port)
            await asyncio.sleep(1)
            if not self.to_process.empty():
                msg = self.to_process.get()
                self.handle_msg(msg)
                await asyncio.sleep(1)
            if not self.connected:
                print("could not connect to master, trying again in 1 second")
            else:
                break

        self.publish("__sub__", subs)
        # sending data to individual processes (async)
        for loop in self.loops:
            asyncio.create_task(self._timer(loop["time"], loop["func"]))
        while self.is_alive:
            if not self.to_process.empty():
                msg = self.to_process.get()
                self.handle_msg(msg)
            await asyncio.sleep(0)  # 1hz?

    # call asyncio
    def run(self):
        asyncio.run(self._run())

    # returns: -1 = error, 0 = already subbed, 1 = first time sub
    def sub(self, topic, cbfunc_name: str):
        try:
            if self.subscriptions.get(topic):
                return 0
            else:
                self.subscriptions[topic] = getattr(self, cbfunc_name)
                return 1
        except AttributeError as e:
            print(f"function \"{cbfunc_name}\" not found!")
            return -1

    @subscribe("__echo__")
    def _echo(self, data, port):
        self.connected = True

    @subscribe("__stop__")
    def _stop(self, data, port):
        print(f"stopping {self.name}")
        for process in self.processes:
            process.terminate()
            self.is_alive = False

    @subscribe("__error__")
    def _error(self, data, port):
        print("error decoding data!")

class MasterNode(Node):

    def __init__(self):
        super().__init__("master", recv_port=["127.0.0.1", "12222"], send_port=["127.0.0.1", "22222"])
        self.topics = {}  # {topic names: subscriber id: str} string is needed for COM ports
        self.statics.append("get_all_subs")

    # returns all nodes that are subscribed to any topic
    def get_all_subs(self):
        uniques = []
        for i in self.topics.values():
            for j in i:
                if j not in uniques:
                    uniques.append(j)
        return uniques

    # initializes the node completely (sends self subscriptions to master)
    # then starts sender and listener processes
    # finally starts the main loop of parsing information in to_process to on_receive
    async def _run(self):
        print(f"{self.name} has an id of {id(self)}")
        self._add_sub()
        listener = multiprocessing.Process(target=run_io_listener, args=[self.to_process, self.recvport])
        sender = multiprocessing.Process(target=run_io_sender, args=[self.to_send, self.sendport])
        self.processes.append(listener)
        self.processes.append(sender)
        listener.start()
        sender.start()
        # loops
        # sending data to individual processes (async)
        while self.is_alive:
            if not self.to_process.empty():
                msg = self.to_process.get()
                self.handle_msg(msg)
            await asyncio.sleep(0)  # 1khz?

    def on_receive(self, topic, data, port):
        if self.topics.get(topic) and topic not in self.subscriptions.keys():
            print(f"master node sending topic {topic} to nodes: {self.topics[topic]}")
            self.to_send.put(
                [json.dumps({"topic": topic, "data": data, "addr": port}).encode("utf-8") + b"\n", self.topics[topic]])
        elif topic in self.subscriptions.keys():
            self.subscriptions[topic](data, port)
        else:
            print(f"topic \"{topic}\" has no recipients")

    @subscribe("__echo__")
    def _echo(self, data, port):
        self.to_send.put(
            [json.dumps({"topic": "__echo__", "data": data, "addr": self.recvport}).encode("utf-8") + b"\n", port])

    @subscribe("__stop__")
    def _stop(self, data, port):
        self.to_send.put(
            [json.dumps({"topic": "__stop__", "data": "stop", "addr": self.recvport}).encode("utf-8") + b"\n",
             self.get_all_subs()])
        for process in self.processes:
            process.terminate()
        self.is_alive = False

    @subscribe("__sub__")
    def _sub(self, data, port):
        # subscriptions
        print(f"setting up subscription data for {data}")
        for t in data:
            try:
                self.topics[t].append(port)
            except KeyError:
                self.topics[t] = [port]
        print(f"current subs: {self.get_all_subs()}")


def main():
    master = MasterNode()
    master.run()


if __name__ == "__main__":
    main()
