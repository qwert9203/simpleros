from node import Node, subscribe


class MinimalSub(Node):

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)

    @subscribe("test_topic")
    def test_sub(self, data, port):
        print(f"received {data}")


def main():
    min_node = MinimalSub("minimal_sub", recv_port=("127.0.0.1", "30002"), send_port=("127.0.0.1", "30003"))
    min_node.run()


if __name__ == "__main__":
    main()
