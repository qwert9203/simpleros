from node import *


class MinimalPub(LocalNode):

    def __init__(self, name):
        super().__init__(name)

    # creates a loop that runs every 1000 ms
    @loop(1000)
    def test_pub(self):  # function to run every 1000 ms
        self.publish("test_topic", time.time())


def main():
    min_node = MinimalPub("minimal_pub")
    min_node.run()


if __name__ == "__main__":
    main()
