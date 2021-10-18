#!/usr/bin/python

counter = 0
while True:
    try:
        # use `raw_input()` for Python 2
        input()
    except EOFError:
        break
    counter += 1
print(counter)
