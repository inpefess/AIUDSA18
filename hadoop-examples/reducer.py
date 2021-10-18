#!/usr/bin/python

counter = 0
while True:
    try:
        # use `raw_input()` for Python 2
        line = input()
    except EOFError:
        break
    counter += int(line)
print(counter)
