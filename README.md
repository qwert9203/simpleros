# simpleros
ROS but it's (will be) a python library


This "library" has a similar architecture to ROS1, but the middleware layer is mostly handled by python itself. 


## "installation"
copy node.py 


## Introduction

Create a node by creating an object that inherits the node.Node or node.LocalNode object

```python
from node import *


class Foo(LocalNode):

    def __init__(self, name):
        super().__init__(name)
        
        
# Boilerplate
def main():
    foo = Foo("bar")
    foo.run()


if __name__ == "__main__":
    main()
```

Adding functionality to the node:


Subscribing to a topic

```python

class Foo(LocalNode):

    def __init__(self, name):
        super().__init__(name)
        
        
    @subscribe("topic_name")
    def any_name_you_want(self, data, port)
        print(f"received {data} from {port}!")

```

Publishing data to a topic

```python
        
    @subscribe("topic_name_2")
    def any_name_you_want_2(self, data, port)
        self.publish("topic_name", "I received data!")

```


Adding a loop to the node

```python
    
    @loop(1000)
    def any_name_you_want_3(self)
        self.publish("topic_name", time.time())

```
