#!/opt/homebrew/bin/python3
import json

filenames = ['nasdaqlisted', 'otherlisted']
symbols = {}
for filename in filenames:
    with open(filename + '.txt') as fd:
        lineno = 0
        for line in fd:
            lineno += 1
            if lineno == 1:
                continue
            if line.startswith('File Creation Time'):
                continue
            tokens = line.split('|')
            symbols[tokens[0]] = True
    with open(filename + '.json', 'w') as fd:
        json.dump(symbols, fd, indent=4)
