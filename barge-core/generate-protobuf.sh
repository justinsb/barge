#!/bin/bash

protoc -Isrc/main/proto src/main/proto/* --java_out=src/main/java
