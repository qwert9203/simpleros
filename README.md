# simpleros
ROS but it's (will be) a python library

## "installation"
copy node.py 

## Usage

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
