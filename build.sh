#!/bin/bash

export GOOS="linux" # windows darwin linux

go build -v -o ./output/resolver-cli ./cmd...
echo "build successfully!"
