import time

from node import *


class MinimalSub(LocalNode):

    def __init__(self, name):
        super().__init__(name)

    # subscribes to "test_topic"
    @subscribe("test_topic")
    def test_sub(self, data, port):  # function to run whenever this node receives data from the topic "test_topic"
        t = time.time()
        print(f"received {data}, took{t-data} seconds")


def main():
    min_node = MinimalSub("minimal_sub")
    min_node.run()
