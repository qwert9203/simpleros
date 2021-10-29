from node import Node, loop


class MinimalPub(Node):

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)

    @loop(1000)
    def test_pub(self):
        self.publish("test_topic", "test_data")


def main():
    min_node = MinimalPub("minimal_pub", recv_port=("127.0.0.1", "30004"), send_port=("127.0.0.1", "30005"))
    min_node.run()


if __name__ == "__main__":
    main()